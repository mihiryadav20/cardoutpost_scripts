package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// TCGdex API types
// ---------------------------------------------------------------------------

type CardIndexEntry struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type CardDetail struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Pricing *Pricing `json:"pricing,omitempty"`
}

type Pricing struct {
	TCGPlayer  *TCGPlayerPricing  `json:"tcgplayer,omitempty"`
	CardMarket *CardMarketPricing `json:"cardmarket,omitempty"`
}

type TCGPlayerPricing struct {
	Updated  string            `json:"updated"`
	Unit     string            `json:"unit"`
	Holofoil *TCGPlayerVariant `json:"holofoil,omitempty"`
	Normal   *TCGPlayerVariant `json:"normal,omitempty"`
	Reverse  *TCGPlayerVariant `json:"reverseHolofoil,omitempty"`
	First    *TCGPlayerVariant `json:"1stEditionHolofoil,omitempty"`
}

type TCGPlayerVariant struct {
	LowPrice       float64 `json:"lowPrice"`
	MidPrice       float64 `json:"midPrice"`
	HighPrice      float64 `json:"highPrice"`
	MarketPrice    float64 `json:"marketPrice"`
	DirectLowPrice float64 `json:"directLowPrice"`
}

type CardMarketPricing struct {
	Updated string  `json:"updated"`
	Unit    string  `json:"unit"`
	Avg     float64 `json:"avg"`
	Low     float64 `json:"low"`
	Trend   float64 `json:"trend"`
}

func (c *CardDetail) bestPrice() float64 {
	if c.Pricing == nil {
		return 0
	}
	if tp := c.Pricing.TCGPlayer; tp != nil {
		for _, v := range []*TCGPlayerVariant{tp.Holofoil, tp.Normal, tp.Reverse, tp.First} {
			if v == nil {
				continue
			}
			if v.MarketPrice > 0 {
				return v.MarketPrice
			}
			if v.MidPrice > 0 {
				return v.MidPrice
			}
			if v.LowPrice > 0 {
				return v.LowPrice
			}
		}
	}
	if cm := c.Pricing.CardMarket; cm != nil {
		if cm.Trend > 0 {
			return cm.Trend
		}
		if cm.Avg > 0 {
			return cm.Avg
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// Simulation card type
// ---------------------------------------------------------------------------

type SimCard struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

// ---------------------------------------------------------------------------
// Pack tier & bucket config
// ---------------------------------------------------------------------------

type Bucket struct {
	Low    float64
	High   float64
	Weight float64 // out of 100
	Cards  []SimCard
}

func (b Bucket) Label() string {
	return fmt.Sprintf("$%.2g-$%.4g", b.Low, b.High)
}

type PackTier struct {
	Name         string
	Price        float64
	CardsPerPack int
	HardCap      float64
	MinValue     float64
	Buckets      []Bucket
}

func buildTiers() []PackTier {
	return []PackTier{
		{
			Name: "Starter", Price: 5, CardsPerPack: 3, HardCap: 25, MinValue: 0.25,
			Buckets: []Bucket{
				{Low: 0.25, High: 0.50, Weight: 70},
				{Low: 0.50, High: 1, Weight: 15},
				{Low: 1, High: 2, Weight: 8},
				{Low: 2, High: 5, Weight: 4},
				{Low: 5, High: 25, Weight: 3},
			},
		},
		{
			Name: "Basic", Price: 15, CardsPerPack: 5, HardCap: 75, MinValue: 0.50,
			Buckets: []Bucket{
				{Low: 0.50, High: 1, Weight: 72},
				{Low: 1, High: 2, Weight: 14},
				{Low: 2, High: 4, Weight: 7},
				{Low: 4, High: 10, Weight: 4},
				{Low: 10, High: 75, Weight: 3},
			},
		},
		{
			Name: "Premium", Price: 25, CardsPerPack: 7, HardCap: 150, MinValue: 0.50,
			Buckets: []Bucket{
				{Low: 0.50, High: 1, Weight: 79},
				{Low: 1, High: 2, Weight: 10},
				{Low: 2, High: 4, Weight: 4},
				{Low: 4, High: 12, Weight: 4},
				{Low: 12, High: 150, Weight: 3},
			},
		},
		{
			Name: "Grail", Price: 50, CardsPerPack: 10, HardCap: 500, MinValue: 1,
			Buckets: []Bucket{
				{Low: 1, High: 2, Weight: 83},
				{Low: 2, High: 3.50, Weight: 7},
				{Low: 3.50, High: 6, Weight: 4},
				{Low: 6, High: 15, Weight: 3},
				{Low: 15, High: 500, Weight: 3},
			},
		},
	}
}

// ---------------------------------------------------------------------------
// HTTP with retry
// ---------------------------------------------------------------------------

var httpClient = &http.Client{Timeout: 30 * time.Second}

func fetchJSON(url string, target interface{}) error {
	const maxRetries = 4
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*attempt) * 500 * time.Millisecond)
		}
		resp, err := httpClient.Get(url)
		if err != nil {
			if attempt == maxRetries {
				return fmt.Errorf("GET %s: %w", url, err)
			}
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			if attempt == maxRetries {
				return fmt.Errorf("GET %s: status %d after retries", url, resp.StatusCode)
			}
			continue
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
		}
		if err != nil {
			return fmt.Errorf("reading body %s: %w", url, err)
		}
		return json.Unmarshal(body, target)
	}
	return fmt.Errorf("GET %s: exhausted retries", url)
}

// ---------------------------------------------------------------------------
// Cache helpers
// ---------------------------------------------------------------------------

const cacheDir = ".cache"

func cachePath() string { return filepath.Join(cacheDir, "cards.json") }

func loadCache() ([]SimCard, bool) {
	data, err := os.ReadFile(cachePath())
	if err != nil {
		return nil, false
	}
	var cards []SimCard
	if err := json.Unmarshal(data, &cards); err != nil || len(cards) == 0 {
		return nil, false
	}
	return cards, true
}

func saveCache(cards []SimCard) {
	os.MkdirAll(cacheDir, 0o755)
	data, _ := json.Marshal(cards)
	os.WriteFile(cachePath(), data, 0o644)
}

// ---------------------------------------------------------------------------
// Fetch all cards
// ---------------------------------------------------------------------------

func fetchAllCards() []SimCard {
	fmt.Println("Step 1: Fetching card index from TCGdex...")

	// Try cache first
	if cards, ok := loadCache(); ok {
		fmt.Printf("  [cache] Loading from cache...\n")
		withPrice := len(cards)
		fmt.Printf("  Cards with price data: %d\n", withPrice)
		if withPrice > 0 {
			prices := make([]float64, withPrice)
			for i, c := range cards {
				prices[i] = c.Price
			}
			sort.Float64s(prices)
			fmt.Printf("  Price range: $%.2f - $%.2f\n", prices[0], prices[len(prices)-1])
			fmt.Printf("  Median price: $%.2f\n", median(prices))
		}
		return cards
	}

	fmt.Printf("  [api] Fetching from API...\n")
	var index []CardIndexEntry
	if err := fetchJSON("https://api.tcgdex.net/v2/en/cards", &index); err != nil {
		fmt.Printf("  ERROR fetching index: %v\n", err)
		os.Exit(1)
	}
	total := len(index)
	fmt.Printf("  Found %d cards in index\n", total)
	fmt.Printf("Step 2: Fetching price data for all %d cards (50 concurrent)...\n", total)

	type result struct {
		card SimCard
		ok   bool
	}
	results := make([]result, total)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 50)
	var mu sync.Mutex
	var done int64

	for i, entry := range index {
		wg.Add(1)
		go func(idx int, e CardIndexEntry) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			time.Sleep(time.Duration(rand.Intn(40)) * time.Millisecond)

			url := fmt.Sprintf("https://api.tcgdex.net/v2/en/cards/%s", e.ID)
			var detail CardDetail
			if err := fetchJSON(url, &detail); err != nil {
				mu.Lock()
				done++
				mu.Unlock()
				return
			}
			price := detail.bestPrice()
			if price > 0 {
				results[idx] = result{
					card: SimCard{ID: e.ID, Name: e.Name, Price: price},
					ok:   true,
				}
			}
			mu.Lock()
			done++
			d := done
			mu.Unlock()
			if d%1000 == 0 || d == int64(total) {
				fmt.Printf("\r  Progress: %d/%d (%.0f%%)", d, total, float64(d)/float64(total)*100)
			}
		}(i, entry)
	}
	wg.Wait()
	fmt.Println()

	var cards []SimCard
	for _, r := range results {
		if r.ok {
			cards = append(cards, r.card)
		}
	}
	sort.Slice(cards, func(i, j int) bool { return cards[i].Price < cards[j].Price })
	saveCache(cards)

	withPrice := len(cards)
	without := total - withPrice
	fmt.Printf("  Cards with price data: %d\n", withPrice)
	fmt.Printf("  Cards without price data: %d (excluded)\n", without)
	if withPrice > 0 {
		prices := make([]float64, withPrice)
		for i, c := range cards {
			prices[i] = c.Price
		}
		fmt.Printf("  Price range: $%.2f - $%.2f\n", prices[0], prices[len(prices)-1])
		fmt.Printf("  Median price: $%.2f\n", median(prices))
	}
	return cards
}

func median(sorted []float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

// ---------------------------------------------------------------------------
// Populate buckets per tier (hard cap enforced)
// ---------------------------------------------------------------------------

func populateBuckets(tiers []PackTier, cards []SimCard) {
	for ti := range tiers {
		tier := &tiers[ti]
		for bi := range tier.Buckets {
			b := &tier.Buckets[bi]
			b.Cards = nil
			for _, c := range cards {
				if c.Price > tier.HardCap {
					continue // hard cap: exclude cards above tier max
				}
				if c.Price >= b.Low && c.Price < b.High {
					b.Cards = append(b.Cards, c)
				}
			}
		}
	}
}

func printBucketSizes(tiers []PackTier) {
	fmt.Println("\nBucket sizes:")
	for _, tier := range tiers {
		fmt.Printf("  %s (max $%.0f):  ", tier.Name, tier.HardCap)
		for i, b := range tier.Buckets {
			if i > 0 {
				fmt.Print("  ")
			}
			fmt.Printf("[$%.4g-$%.4g]=%d", b.Low, b.High, len(b.Cards))
		}
		fmt.Println()
	}
	fmt.Println()
}

// ---------------------------------------------------------------------------
// Simulation engine
// ---------------------------------------------------------------------------

type TierRunResult struct {
	TierName       string
	PackPrice      float64
	CardsPerPack   int
	NumPacks       int
	ProfitablePacks int
	TotalRevenue   float64
	TotalPayout    float64
	MinPayout      float64
	MaxPayout      float64
}

func (r TierRunResult) ProfitPct() float64 {
	return float64(r.ProfitablePacks) / float64(r.NumPacks) * 100
}

func (r TierRunResult) MarginPct() float64 {
	if r.TotalRevenue == 0 {
		return 0
	}
	return (r.TotalRevenue - r.TotalPayout) / r.TotalRevenue * 100
}

func simulateTier(rng *rand.Rand, tier PackTier, numPacks int) TierRunResult {
	// Build cumulative weights (out of 100)
	cumWeights := make([]float64, len(tier.Buckets))
	cumWeights[0] = tier.Buckets[0].Weight
	for i := 1; i < len(tier.Buckets); i++ {
		cumWeights[i] = cumWeights[i-1] + tier.Buckets[i].Weight
	}
	totalWeight := cumWeights[len(cumWeights)-1]

	res := TierRunResult{
		TierName:     tier.Name,
		PackPrice:    tier.Price,
		CardsPerPack: tier.CardsPerPack,
		NumPacks:     numPacks,
		TotalRevenue: float64(numPacks) * tier.Price,
		MinPayout:    math.MaxFloat64,
		MaxPayout:    0,
	}

	for p := 0; p < numPacks; p++ {
		packPayout := 0.0

		for card := 0; card < tier.CardsPerPack; card++ {
			// Pick a bucket via weighted random
			roll := rng.Float64() * totalWeight
			bucketIdx := 0
			for i, cw := range cumWeights {
				if roll <= cw {
					bucketIdx = i
					break
				}
			}

			bucket := &tier.Buckets[bucketIdx]
			if len(bucket.Cards) == 0 {
				// Fallback: use largest non-empty bucket
				bucket = largestBucket(&tier)
				if bucket == nil || len(bucket.Cards) == 0 {
					continue
				}
			}

			// Pick random card, re-roll if below min value (up to 20 tries)
			var picked SimCard
			found := false
			for attempt := 0; attempt < 20; attempt++ {
				picked = bucket.Cards[rng.Intn(len(bucket.Cards))]
				if picked.Price >= tier.MinValue {
					found = true
					break
				}
			}
			if !found {
				continue // skip this card slot
			}

			price := picked.Price
			if price > tier.HardCap {
				price = tier.HardCap
			}
			packPayout += price
		}

		res.TotalPayout += packPayout
		if packPayout < res.MinPayout {
			res.MinPayout = packPayout
		}
		if packPayout > res.MaxPayout {
			res.MaxPayout = packPayout
		}
		if tier.Price > packPayout {
			res.ProfitablePacks++
		}
	}

	if res.MinPayout == math.MaxFloat64 {
		res.MinPayout = 0
	}
	return res
}

func largestBucket(tier *PackTier) *Bucket {
	var best *Bucket
	bestLen := 0
	for i := range tier.Buckets {
		if len(tier.Buckets[i].Cards) > bestLen {
			best = &tier.Buckets[i]
			bestLen = len(tier.Buckets[i].Cards)
		}
	}
	return best
}

// ---------------------------------------------------------------------------
// Aggregated results across runs
// ---------------------------------------------------------------------------

type TierAggregate struct {
	Name         string
	Price        float64
	CardsPerPack int
	Runs         []TierRunResult
}

func (a TierAggregate) AvgProfitPct() float64 {
	sum := 0.0
	for _, r := range a.Runs {
		sum += r.ProfitPct()
	}
	return sum / float64(len(a.Runs))
}

func (a TierAggregate) MinProfitPct() float64 {
	m := math.MaxFloat64
	for _, r := range a.Runs {
		v := r.ProfitPct()
		if v < m {
			m = v
		}
	}
	return m
}

func (a TierAggregate) MaxProfitPct() float64 {
	m := -math.MaxFloat64
	for _, r := range a.Runs {
		v := r.ProfitPct()
		if v > m {
			m = v
		}
	}
	return m
}

func (a TierAggregate) AvgMargin() float64 {
	sum := 0.0
	for _, r := range a.Runs {
		sum += r.MarginPct()
	}
	return sum / float64(len(a.Runs))
}

func (a TierAggregate) AvgPackPayout() float64 {
	var totalPayout float64
	var totalPacks int
	for _, r := range a.Runs {
		totalPayout += r.TotalPayout
		totalPacks += r.NumPacks
	}
	return totalPayout / float64(totalPacks)
}

func (a TierAggregate) GlobalMinPayout() float64 {
	m := math.MaxFloat64
	for _, r := range a.Runs {
		if r.MinPayout < m {
			m = r.MinPayout
		}
	}
	return m
}

func (a TierAggregate) GlobalMaxPayout() float64 {
	m := 0.0
	for _, r := range a.Runs {
		if r.MaxPayout > m {
			m = r.MaxPayout
		}
	}
	return m
}

func (a TierAggregate) RunsPassed() int {
	n := 0
	for _, r := range a.Runs {
		if r.ProfitPct() >= 85 {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	seedFlag := flag.Int("seed", 0, "Base seed (0 = auto-generate)")
	flag.Parse()

	// ---- Step 1 & 2: Fetch cards ----
	cards := fetchAllCards()
	if len(cards) == 0 {
		fmt.Println("FATAL: No cards with price data. Cannot simulate.")
		os.Exit(1)
	}

	// ---- Build tiers & populate buckets ----
	tiers := buildTiers()
	populateBuckets(tiers, cards)
	printBucketSizes(tiers)

	// ---- Seeds ----
	const numRuns = 10
	const packsPerTier = 50000

	seeds := make([]int64, numRuns)
	if *seedFlag != 0 {
		for i := range seeds {
			seeds[i] = int64(*seedFlag) + int64(i*7919) // spread with a prime
		}
	} else {
		src := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := range seeds {
			seeds[i] = src.Int63n(1_000_000_000)
		}
	}

	totalPacks := numRuns * len(tiers) * packsPerTier
	fmt.Printf("Simulating %d total pack openings (%d runs x %d tiers x %d packs)...\n\n",
		totalPacks, numRuns, len(tiers), packsPerTier)

	// ---- Aggregates ----
	aggs := make(map[string]*TierAggregate)
	for _, t := range tiers {
		aggs[t.Name] = &TierAggregate{
			Name:         t.Name,
			Price:        t.Price,
			CardsPerPack: t.CardsPerPack,
		}
	}

	simStart := time.Now()

	// ---- Run simulations ----
	for run := 0; run < numRuns; run++ {
		seed := seeds[run]
		// Run all 4 tiers concurrently within this run
		results := make([]TierRunResult, len(tiers))
		var wg sync.WaitGroup
		for ti, tier := range tiers {
			wg.Add(1)
			go func(idx int, t PackTier, s int64) {
				defer wg.Done()
				// Each tier gets its own deterministic RNG derived from run seed + tier index
				rng := rand.New(rand.NewSource(s + int64(idx)*31))
				results[idx] = simulateTier(rng, t, packsPerTier)
			}(ti, tier, seed)
		}
		wg.Wait()

		// Print compact per-run line
		fmt.Printf("Run %2d |", run+1)
		for _, r := range results {
			fmt.Printf(" %s: %.1f%% profitable, %.1f%% margin |",
				r.TierName, r.ProfitPct(), r.MarginPct())
			aggs[r.TierName].Runs = append(aggs[r.TierName].Runs, r)
		}
		fmt.Println()
	}

	simElapsed := time.Since(simStart)

	// ---- Final summary ----
	fmt.Printf("\n=== FINAL RESULTS (%s pack openings) ===\n", formatInt(totalPacks))

	tierOrder := []string{"Starter", "Basic", "Premium", "Grail"}
	allPassed := true
	for _, name := range tierOrder {
		a := aggs[name]
		passed := a.RunsPassed()
		if passed < numRuns {
			allPassed = false
		}
		fmt.Printf("\n%s ($%.0f, %d cards):\n", a.Name, a.Price, a.CardsPerPack)
		fmt.Printf("  Avg profitability: %.1f%% (range: %.1f%% - %.1f%%)\n",
			a.AvgProfitPct(), a.MinProfitPct(), a.MaxProfitPct())
		fmt.Printf("  Avg margin: %.1f%%\n", a.AvgMargin())
		fmt.Printf("  Avg pack payout: $%.2f\n", a.AvgPackPayout())
		fmt.Printf("  Min pack payout seen: $%.2f\n", a.GlobalMinPayout())
		fmt.Printf("  Max pack payout seen: $%.2f\n", a.GlobalMaxPayout())
		fmt.Printf("  Runs passed (>=85%%): %d/%d\n", passed, numRuns)
	}

	fmt.Println()
	if allPassed {
		fmt.Println("ALL TIERS PASSED ALL RUNS: YES")
	} else {
		fmt.Println("ALL TIERS PASSED ALL RUNS: NO")
	}
	fmt.Printf("Completed in %dms\n", simElapsed.Milliseconds())
}

func formatInt(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var out []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}
