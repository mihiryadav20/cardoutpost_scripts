package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ── TCGDex cache types ──

type TCGDexCard struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

// ── PokeTrace API types ──

type PTSearchResponse struct {
	Data       []PTCard     `json:"data"`
	Pagination PTPagination `json:"pagination"`
}

type PTPagination struct {
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor"`
	Count      int    `json:"count"`
}

type PTCard struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	CardNumber string            `json:"cardNumber"`
	Set        PTSet             `json:"set"`
	Variant    *string           `json:"variant"`
	Rarity     string            `json:"rarity"`
	Image      string            `json:"image"`
	Market     string            `json:"market"`
	Currency   string            `json:"currency"`
	Prices     map[string]PTTier `json:"prices"`
	Updated    string            `json:"lastUpdated"`
}

type PTSet struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

type PTTier map[string]PTConditionStats

type PTConditionStats struct {
	Avg         float64 `json:"avg"`
	Low         float64 `json:"low"`
	High        float64 `json:"high"`
	SaleCount   int     `json:"saleCount"`
	LastUpdated string  `json:"lastUpdated"`
	Avg1d       float64 `json:"avg1d"`
	Avg7d       float64 `json:"avg7d"`
	Avg30d      float64 `json:"avg30d"`
}

// ── Output types (26 fields) ──

type ScoredCard struct {
	TcgdexID           string  `json:"tcgdex_id"`
	PoketraceID        string  `json:"poketrace_id"`
	Name               string  `json:"name"`
	Set                string  `json:"set"`
	TcgdexPrice        float64 `json:"tcgdex_price"`
	PoketraceAvgNM     float64 `json:"poketrace_avg_nm"`
	TotalSaleCount     int     `json:"total_sale_count"`
	EbaySaleCount      int     `json:"ebay_sale_count"`
	TcgplayerSaleCount int     `json:"tcgplayer_sale_count"`
	PlatformCount      int     `json:"platform_count"`
	PriceStability     float64 `json:"price_stability"`
	FreshnessDays      int     `json:"freshness_days"`
	LiquidityScore     float64 `json:"liquidity_score"`
	Matched            bool    `json:"matched"`

	SetSlug    string  `json:"set_slug"`
	CardNumber string  `json:"card_number"`
	Variant    *string `json:"variant"`
	Rarity     string  `json:"rarity"`
	ImageURL   string  `json:"image_url"`

	LPAvgPrice  float64 `json:"lp_avg_price"`
	NMSaleCount int     `json:"nm_sale_count"`
	NMAvg7d     float64 `json:"nm_avg7d"`
	NMAvg30d    float64 `json:"nm_avg30d"`

	PriceTrend float64 `json:"price_trend"`
	PackScore  float64 `json:"pack_score"`
	HasGraded  bool    `json:"has_graded"`

	PSA10Avg       float64 `json:"psa_10_avg"`
	PSA10SaleCount int     `json:"psa_10_sale_count"`
	PSA10Avg7d     float64 `json:"psa_10_avg7d"`
	PSA10Avg30d    float64 `json:"psa_10_avg30d"`
	PSA9Avg        float64 `json:"psa_9_avg"`
	PSA9SaleCount  int     `json:"psa_9_sale_count"`
	PSA9Avg7d      float64 `json:"psa_9_avg7d"`
	PSA9Avg30d     float64 `json:"psa_9_avg30d"`

	PrevSaleCount *int     `json:"prev_sale_count"`
	PrevScanDate  *string  `json:"prev_scan_date"`
	SalesPerWeek  *float64 `json:"sales_per_week"`

	DedupReason *string `json:"dedup_reason,omitempty"`
}

type OutputFile struct {
	ScanTimestamp string       `json:"scan_timestamp"`
	Cards        []ScoredCard `json:"cards"`
}

type ProgressFile struct {
	ProcessedIDs []string     `json:"processed_ids"`
	Results      []ScoredCard `json:"results"`
	LastIndex    int          `json:"last_index"`
}

// ── Paths & config ──

const (
	envPath         = "../.env"
	tcgdexCachePath = "../TCGDex_filtering/.cache/cards.json"
	progressPath    = ".cache/liquidity_progress.json"
	outputPath      = ".cache/liquidity_scored.json"
	baseURL         = "https://api.poketrace.com/v1"
	checkpointEvery = 50
	numWorkers      = 10
)

var (
	apiKey          string
	rateLimitRemain int64 = -1 // atomic, unknown until first response
	stopFlag        int64      // atomic, set to 1 on shutdown/rate-limit
)

// work item sent to workers
type workItem struct {
	index int
	card  TCGDexCard
}

// result from a worker
type workResult struct {
	index  int
	scored ScoredCard
}

func main() {
	startTime := time.Now()

	apiKey = loadAPIKey()
	if apiKey == "" {
		fmt.Println("ERROR: POKETRACE_API_KEY not found.")
		fmt.Println("  Set it in ../.env (POKETRACE_API_KEY=your_key) or as an env var.")
		os.Exit(1)
	}

	// Load TCGDex cache
	cards := loadTCGDexCache()
	fmt.Printf("Loaded %d cards from TCGDex cache\n", len(cards))

	// Filter $1+
	var filtered []TCGDexCard
	for _, c := range cards {
		if c.Price >= 1.0 {
			filtered = append(filtered, c)
		}
	}
	fmt.Printf("Cards $1+: %d\n", len(filtered))

	// Load previous scan for velocity tracking
	prevScan := loadPreviousScan()
	if len(prevScan) > 0 {
		fmt.Printf("Previous scan loaded: %d cards (for velocity tracking)\n", len(prevScan))
	} else {
		fmt.Println("No previous scan found — velocity data will be available after the next run.")
	}

	// Load progress (resume support)
	progress := loadProgress()
	processedSet := make(map[string]bool)
	for _, id := range progress.ProcessedIDs {
		processedSet[id] = true
	}
	results := progress.Results
	if len(results) > 0 {
		fmt.Printf("Resuming: %d cards already processed\n", len(results))
	}

	// Build work queue — skip already processed
	var toProcess []workItem
	for i, card := range filtered {
		if !processedSet[card.ID] {
			toProcess = append(toProcess, workItem{index: i, card: card})
		}
	}
	fmt.Printf("Cards to process: %d (with %d concurrent workers)\n", len(toProcess), numWorkers)

	if len(toProcess) == 0 {
		fmt.Println("Nothing to process.")
		results = deduplicateResults(results)
		normalizeScores(results)
		saveOutput(results)
		saveCSV(results)
		printSummary(results, len(filtered), prevScan)
		return
	}

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n  Caught interrupt, finishing in-flight requests...")
		atomic.StoreInt64(&stopFlag, 1)
	}()

	// Rate limiter: 30 requests per 10 seconds = 1 token every 334ms
	rateLimiter := time.NewTicker(334 * time.Millisecond)
	defer rateLimiter.Stop()

	// Channels
	jobs := make(chan workItem, len(toProcess))
	resultsChan := make(chan workResult, len(toProcess))

	// Feed jobs
	go func() {
		for _, w := range toProcess {
			if atomic.LoadInt64(&stopFlag) == 1 {
				break
			}
			jobs <- w
		}
		close(jobs)
	}()

	// Launch workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if atomic.LoadInt64(&stopFlag) == 1 {
					return
				}

				// Wait for rate limit token
				<-rateLimiter.C

				// Check rate limit remaining
				remain := atomic.LoadInt64(&rateLimitRemain)
				if remain >= 0 && remain <= 10 {
					fmt.Printf("\n  Rate limit nearly exhausted (%d remaining), stopping.\n", remain)
					atomic.StoreInt64(&stopFlag, 1)
					return
				}

				scored, err := enrichCard(job.card, prevScan)
				if err != nil {
					fmt.Printf("  [%d/%d] ERROR %s: %v\n", job.index+1, len(filtered), job.card.Name, err)
					scored = unmatchedCard(job.card, prevScan)
				}

				resultsChan <- workResult{index: job.index, scored: scored}
			}
		}()
	}

	// Close results channel when all workers done
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	mu := &sync.Mutex{}
	processed := len(results)
	matched := 0
	unmatched := 0
	for _, r := range results {
		if r.Matched {
			matched++
		} else {
			unmatched++
		}
	}

	for res := range resultsChan {
		mu.Lock()
		results = append(results, res.scored)
		processedSet[res.scored.TcgdexID] = true
		processed++

		if res.scored.Matched {
			matched++
			fmt.Printf("  [%d/%d] ✓ %s — sales:%d\n",
				processed, len(filtered), res.scored.Name, res.scored.TotalSaleCount)
		} else {
			unmatched++
			fmt.Printf("  [%d/%d] ✗ %s — unmatched\n", processed, len(filtered), res.scored.Name)
		}

		// Checkpoint
		if processed%checkpointEvery == 0 {
			saveProgress(results, processedSet)
			fmt.Printf("  Checkpoint saved (%d cards)\n", processed)
		}
		mu.Unlock()
	}

	// Final save
	results = deduplicateResults(results)
	normalizeScores(results)
	saveProgress(results, processedSet)
	saveOutput(results)
	saveCSV(results)
	printSummary(results, len(filtered), prevScan)

	elapsed := time.Since(startTime)
	fmt.Printf("\n  Completed in %s\n", elapsed.Round(time.Second))
}

// ── .env + API key loading ──

func loadAPIKey() string {
	data, err := os.ReadFile(envPath)
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "POKETRACE_API_KEY=") {
				return strings.TrimPrefix(line, "POKETRACE_API_KEY=")
			}
		}
	}
	return os.Getenv("POKETRACE_API_KEY")
}

// ── TCGDex cache loading ──

func loadTCGDexCache() []TCGDexCard {
	data, err := os.ReadFile(tcgdexCachePath)
	if err != nil {
		fmt.Printf("ERROR: Cannot read TCGDex cache at %s\n", tcgdexCachePath)
		fmt.Println("  Run TCGDex_filtering first: cd ../TCGDex_filtering && go run main.go")
		os.Exit(1)
	}
	var cards []TCGDexCard
	if err := json.Unmarshal(data, &cards); err != nil {
		fmt.Printf("ERROR: Invalid JSON in TCGDex cache: %v\n", err)
		os.Exit(1)
	}
	return cards
}

// ── Previous scan loading (for velocity) ──

type prevScanMap = map[string]prevScanEntry

type prevScanEntry struct {
	TotalSaleCount int
	ScanDate       string
}

func loadPreviousScan() prevScanMap {
	data, err := os.ReadFile(outputPath)
	if err != nil {
		return nil
	}

	var wrapper OutputFile
	if err := json.Unmarshal(data, &wrapper); err == nil && wrapper.ScanTimestamp != "" {
		m := make(prevScanMap)
		for _, c := range wrapper.Cards {
			m[c.TcgdexID] = prevScanEntry{
				TotalSaleCount: c.TotalSaleCount,
				ScanDate:       wrapper.ScanTimestamp,
			}
		}
		return m
	}

	var cards []ScoredCard
	if err := json.Unmarshal(data, &cards); err == nil && len(cards) > 0 {
		m := make(prevScanMap)
		info, statErr := os.Stat(outputPath)
		scanDate := time.Now().UTC().Format(time.RFC3339)
		if statErr == nil {
			scanDate = info.ModTime().UTC().Format(time.RFC3339)
		}
		for _, c := range cards {
			m[c.TcgdexID] = prevScanEntry{
				TotalSaleCount: c.TotalSaleCount,
				ScanDate:       scanDate,
			}
		}
		return m
	}

	return nil
}

// ── Progress management ──

func loadProgress() ProgressFile {
	data, err := os.ReadFile(progressPath)
	if err != nil {
		return ProgressFile{}
	}
	var p ProgressFile
	if err := json.Unmarshal(data, &p); err != nil {
		return ProgressFile{}
	}
	return p
}

func saveProgress(results []ScoredCard, processedSet map[string]bool) {
	ids := make([]string, 0, len(processedSet))
	for id := range processedSet {
		ids = append(ids, id)
	}
	p := ProgressFile{
		ProcessedIDs: ids,
		Results:      results,
		LastIndex:    len(results),
	}
	data, _ := json.MarshalIndent(p, "", "  ")
	os.WriteFile(progressPath, data, 0644)
}

func saveOutput(results []ScoredCard) {
	sorted := make([]ScoredCard, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LiquidityScore > sorted[j].LiquidityScore
	})

	output := OutputFile{
		ScanTimestamp: time.Now().UTC().Format(time.RFC3339),
		Cards:        sorted,
	}
	data, _ := json.MarshalIndent(output, "", "  ")
	os.WriteFile(outputPath, data, 0644)
}

// ── Deduplication by poketrace_id ──

func deduplicateResults(results []ScoredCard) []ScoredCard {
	// Group matched cards by poketrace_id
	groups := make(map[string][]int) // poketrace_id -> indices
	for i, r := range results {
		if r.Matched && r.PoketraceID != "" {
			groups[r.PoketraceID] = append(groups[r.PoketraceID], i)
		}
	}

	dupCount := 0
	for ptID, indices := range groups {
		if len(indices) <= 1 {
			continue
		}

		// Find the best match: closest TCGDex price to PokeTrace NM avg
		bestIdx := indices[0]
		bestDiff := math.Abs(results[indices[0]].TcgdexPrice - results[indices[0]].PoketraceAvgNM)
		for _, idx := range indices[1:] {
			diff := math.Abs(results[idx].TcgdexPrice - results[idx].PoketraceAvgNM)
			if diff < bestDiff {
				bestDiff = diff
				bestIdx = idx
			}
		}

		// Demote all others
		for _, idx := range indices {
			if idx == bestIdx {
				continue
			}
			results[idx].Matched = false
			reason := fmt.Sprintf("duplicate of %s (kept %s)", ptID, results[bestIdx].TcgdexID)
			results[idx].DedupReason = &reason
			results[idx].LiquidityScore = 0
			results[idx].PackScore = 0
			dupCount++
		}
	}

	if dupCount > 0 {
		fmt.Printf("  Dedup: removed %d duplicate matches (kept best price match per PokeTrace card)\n", dupCount)
	}

	return results
}

// ── Score normalization (two-pass) ──

func normalizeScores(results []ScoredCard) {
	maxSales := 0
	for _, r := range results {
		if r.Matched && r.TotalSaleCount > maxSales {
			maxSales = r.TotalSaleCount
		}
	}
	if maxSales == 0 {
		return
	}

	for i := range results {
		if !results[i].Matched {
			continue
		}
		r := &results[i]

		normalizedSales := math.Min(100, float64(r.TotalSaleCount)/float64(maxSales)*100)

		platformBonus := 0.0
		if r.PlatformCount == 2 {
			platformBonus = 100
		} else if r.PlatformCount == 1 {
			platformBonus = 50
		}

		stabilityScore := math.Max(0, 100-r.PriceStability)
		if stabilityScore > 100 {
			stabilityScore = 100
		}

		freshnessScore := 0.0
		if r.FreshnessDays < 7 {
			freshnessScore = 100
		} else if r.FreshnessDays < 30 {
			freshnessScore = 50
		}

		r.LiquidityScore = math.Round(((normalizedSales*0.5)+
			(platformBonus*0.2)+
			(stabilityScore*0.2)+
			(freshnessScore*0.1))*10) / 10

		if r.TcgdexPrice > 0 {
			r.PackScore = math.Round(r.LiquidityScore*math.Log10(r.TcgdexPrice)*10) / 10
		} else {
			r.PackScore = 0
		}
	}
}

// ── PokeTrace API ──

func enrichCard(card TCGDexCard, prev prevScanMap) (ScoredCard, error) {
	ptCards, err := searchPokeTrace(card.Name)
	if err != nil {
		return ScoredCard{}, err
	}

	if len(ptCards) == 0 {
		return unmatchedCard(card, prev), nil
	}

	best := findBestMatch(card, ptCards)
	if best == nil {
		return unmatchedCard(card, prev), nil
	}

	return scoreCard(card, *best, prev), nil
}

func searchPokeTrace(cardName string) ([]PTCard, error) {
	endpoint := fmt.Sprintf("%s/cards?search=%s&market=US&limit=5",
		baseURL, url.QueryEscape(cardName))

	var lastErr error
	backoffs := []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second}

	for attempt := 0; attempt <= 3; attempt++ {
		if attempt > 0 {
			fmt.Printf("    Retry %d/3 after %s...\n", attempt, backoffs[attempt-1])
			time.Sleep(backoffs[attempt-1])
		}

		req, _ := http.NewRequest("GET", endpoint, nil)
		req.Header.Set("X-API-Key", apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("HTTP error: %w", err)
			continue
		}

		// Track rate limit (atomic for concurrency)
		if rl := resp.Header.Get("X-RateLimit-Remaining"); rl != "" {
			if val, err := strconv.Atoi(rl); err == nil {
				atomic.StoreInt64(&rateLimitRemain, int64(val))
			}
		}

		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			resp.Body.Close()
			continue
		}

		if resp.StatusCode != 200 {
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		var result PTSearchResponse
		err = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("JSON decode: %w", err)
		}

		return result.Data, nil
	}

	return nil, fmt.Errorf("all retries exhausted: %w", lastErr)
}

// ── Matching ──

func findBestMatch(tcgCard TCGDexCard, ptCards []PTCard) *PTCard {
	tcgSetID := ""
	if idx := strings.LastIndex(tcgCard.ID, "-"); idx > 0 {
		tcgSetID = tcgCard.ID[:idx]
	}

	type scored struct {
		card  *PTCard
		score int
		sales int
	}

	var candidates []scored

	for i := range ptCards {
		pt := &ptCards[i]
		s := 0

		if strings.EqualFold(pt.Name, tcgCard.Name) {
			s += 100
		} else if strings.Contains(strings.ToLower(pt.Name), strings.ToLower(tcgCard.Name)) {
			s += 50
		} else {
			continue
		}

		if tcgSetID != "" {
			ptSetNorm := normalizeSetName(pt.Set.Name)
			tcgSetNorm := normalizeSetName(tcgSetID)
			if strings.Contains(ptSetNorm, tcgSetNorm) || strings.Contains(tcgSetNorm, ptSetNorm) {
				s += 50
			} else if fuzzySetMatch(pt.Set.Name, tcgSetID) {
				s += 25
			}
		}

		totalSales := countTotalSales(pt)
		candidates = append(candidates, scored{card: pt, score: s, sales: totalSales})
	}

	if len(candidates) == 0 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].sales > candidates[j].sales
	})

	return candidates[0].card
}

func normalizeSetName(name string) string {
	s := strings.ToLower(name)
	s = strings.ReplaceAll(s, "&", "and")
	s = strings.ReplaceAll(s, "-", " ")
	s = strings.ReplaceAll(s, "_", " ")
	s = strings.ReplaceAll(s, "'", "")
	s = strings.ReplaceAll(s, "é", "e")
	return s
}

func fuzzySetMatch(ptSet, tcgSet string) bool {
	a := normalizeSetName(ptSet)
	b := normalizeSetName(tcgSet)

	aWords := strings.Fields(a)
	bWords := strings.Fields(b)

	matchCount := 0
	for _, aw := range aWords {
		if len(aw) < 3 {
			continue
		}
		for _, bw := range bWords {
			if aw == bw {
				matchCount++
				break
			}
		}
	}

	minLen := len(aWords)
	if len(bWords) < minLen {
		minLen = len(bWords)
	}
	if minLen == 0 {
		return false
	}

	return float64(matchCount)/float64(minLen) >= 0.5
}

// ── Scoring (26 fields) ──

func scoreCard(tcg TCGDexCard, pt PTCard, prev prevScanMap) ScoredCard {
	ebaySales := countSourceSales(pt, "ebay")
	tcgpSales := countSourceSales(pt, "tcgplayer")
	totalSales := ebaySales + tcgpSales

	platformCount := 0
	if ebaySales > 0 {
		platformCount++
	}
	if tcgpSales > 0 {
		platformCount++
	}

	avgNM, stability := getPriceStability(pt)
	freshDays := getFreshnessDays(pt)

	lpAvg := getConditionAvg(pt, "LIGHTLY_PLAYED")
	nmSales := getConditionSaleCount(pt, "NEAR_MINT")
	nmAvg7d := getConditionField(pt, "NEAR_MINT", "avg7d")
	nmAvg30d := getConditionField(pt, "NEAR_MINT", "avg30d")

	priceTrend := 0.0
	if nmAvg30d > 0 {
		priceTrend = math.Round(((nmAvg7d-nmAvg30d)/nmAvg30d*100)*100) / 100
	}

	// Raw liquidity for checkpoint (normalization happens at the end)
	platformBonus := 0.0
	if platformCount == 2 {
		platformBonus = 100
	} else if platformCount == 1 {
		platformBonus = 50
	}
	stabilityScore := math.Max(0, 100-stability)
	if stabilityScore > 100 {
		stabilityScore = 100
	}
	freshnessScore := 0.0
	if freshDays < 7 {
		freshnessScore = 100
	} else if freshDays < 30 {
		freshnessScore = 50
	}
	rawLiquidity := (float64(totalSales) * 0.5) +
		(platformBonus * 0.2) +
		(stabilityScore * 0.2) +
		(freshnessScore * 0.1)
	rawLiquidity = math.Round(rawLiquidity*10) / 10

	packScore := 0.0
	if tcg.Price > 0 {
		packScore = math.Round(rawLiquidity*math.Log10(tcg.Price)*10) / 10
	}

	hasGraded := detectGradedTiers(pt)

	// PSA grade extraction
	psa10 := getGradedStats(pt, "PSA_10")
	psa9 := getGradedStats(pt, "PSA_9")

	var variant *string
	if pt.Variant != nil && *pt.Variant != "" {
		variant = pt.Variant
	}

	stability = math.Round(stability*10) / 10

	sc := ScoredCard{
		TcgdexID:           tcg.ID,
		PoketraceID:        pt.ID,
		Name:               tcg.Name,
		Set:                pt.Set.Name,
		TcgdexPrice:        tcg.Price,
		PoketraceAvgNM:     avgNM,
		TotalSaleCount:     totalSales,
		EbaySaleCount:      ebaySales,
		TcgplayerSaleCount: tcgpSales,
		PlatformCount:      platformCount,
		PriceStability:     stability,
		FreshnessDays:      freshDays,
		LiquidityScore:     rawLiquidity,
		Matched:            true,

		SetSlug:    pt.Set.Slug,
		CardNumber: pt.CardNumber,
		Variant:    variant,
		Rarity:     pt.Rarity,
		ImageURL:   pt.Image,

		LPAvgPrice:  lpAvg,
		NMSaleCount: nmSales,
		NMAvg7d:     nmAvg7d,
		NMAvg30d:    nmAvg30d,

		PriceTrend: priceTrend,
		PackScore:  packScore,
		HasGraded:  hasGraded,

		PSA10Avg:       psa10.Avg,
		PSA10SaleCount: psa10.SaleCount,
		PSA10Avg7d:     psa10.Avg7d,
		PSA10Avg30d:    psa10.Avg30d,
		PSA9Avg:        psa9.Avg,
		PSA9SaleCount:  psa9.SaleCount,
		PSA9Avg7d:      psa9.Avg7d,
		PSA9Avg30d:     psa9.Avg30d,
	}

	applyVelocity(&sc, prev)
	return sc
}

func unmatchedCard(tcg TCGDexCard, prev prevScanMap) ScoredCard {
	sc := ScoredCard{
		TcgdexID:    tcg.ID,
		Name:        tcg.Name,
		TcgdexPrice: tcg.Price,
		Matched:     false,
	}
	applyVelocity(&sc, prev)
	return sc
}

// ── Velocity tracking ──

func applyVelocity(sc *ScoredCard, prev prevScanMap) {
	if prev == nil {
		return
	}
	entry, ok := prev[sc.TcgdexID]
	if !ok {
		return
	}

	prevCount := entry.TotalSaleCount
	sc.PrevSaleCount = &prevCount
	sc.PrevScanDate = &entry.ScanDate

	if sc.TotalSaleCount < prevCount {
		return
	}

	prevTime, err := time.Parse(time.RFC3339, entry.ScanDate)
	if err != nil {
		return
	}
	daysBetween := time.Since(prevTime).Hours() / 24
	if daysBetween < 1 {
		return
	}

	velocity := float64(sc.TotalSaleCount-prevCount) / daysBetween * 7
	velocity = math.Round(velocity*10) / 10
	sc.SalesPerWeek = &velocity
}

// ── Field extraction helpers ──

func getConditionAvg(pt PTCard, condition string) float64 {
	for _, source := range []string{"tcgplayer", "ebay"} {
		tier, ok := pt.Prices[source]
		if !ok {
			continue
		}
		stats, ok := tier[condition]
		if ok && stats.Avg > 0 {
			return stats.Avg
		}
	}
	return 0
}

func getConditionSaleCount(pt PTCard, condition string) int {
	total := 0
	for _, source := range []string{"tcgplayer", "ebay"} {
		tier, ok := pt.Prices[source]
		if !ok {
			continue
		}
		stats, ok := tier[condition]
		if ok {
			total += stats.SaleCount
		}
	}
	return total
}

func getConditionField(pt PTCard, condition string, field string) float64 {
	for _, source := range []string{"tcgplayer", "ebay"} {
		tier, ok := pt.Prices[source]
		if !ok {
			continue
		}
		stats, ok := tier[condition]
		if !ok {
			continue
		}
		switch field {
		case "avg7d":
			if stats.Avg7d > 0 {
				return stats.Avg7d
			}
		case "avg30d":
			if stats.Avg30d > 0 {
				return stats.Avg30d
			}
		}
	}
	return 0
}

type gradedStats struct {
	Avg       float64
	SaleCount int
	Avg7d     float64
	Avg30d    float64
}

func getGradedStats(pt PTCard, grade string) gradedStats {
	var combined gradedStats
	for _, source := range []string{"ebay", "tcgplayer"} {
		tier, ok := pt.Prices[source]
		if !ok {
			continue
		}
		stats, ok := tier[grade]
		if !ok {
			continue
		}
		combined.SaleCount += stats.SaleCount
		// Use the first source with a valid avg as the price reference
		if combined.Avg == 0 && stats.Avg > 0 {
			combined.Avg = stats.Avg
			combined.Avg7d = stats.Avg7d
			combined.Avg30d = stats.Avg30d
		}
	}
	return combined
}

func detectGradedTiers(pt PTCard) bool {
	gradedPrefixes := []string{"PSA", "CGC", "BGS", "SGC", "ACE", "TAG", "PCA", "SFG", "CGS"}
	for _, tier := range pt.Prices {
		for key := range tier {
			for _, prefix := range gradedPrefixes {
				if strings.HasPrefix(key, prefix+"_") {
					return true
				}
			}
		}
	}
	return false
}

func countTotalSales(pt *PTCard) int {
	total := 0
	for _, tier := range pt.Prices {
		for _, cond := range tier {
			total += cond.SaleCount
		}
	}
	return total
}

func countSourceSales(pt PTCard, source string) int {
	tier, ok := pt.Prices[source]
	if !ok {
		return 0
	}
	total := 0
	for _, cond := range tier {
		total += cond.SaleCount
	}
	return total
}

func getPriceStability(pt PTCard) (avgNM float64, stability float64) {
	conditionPriority := []string{"NEAR_MINT", "LIGHTLY_PLAYED", "MODERATELY_PLAYED", "HEAVILY_PLAYED", "DAMAGED"}
	sourcePriority := []string{"tcgplayer", "ebay"}

	for _, source := range sourcePriority {
		tier, ok := pt.Prices[source]
		if !ok {
			continue
		}
		for _, cond := range conditionPriority {
			stats, ok := tier[cond]
			if !ok || stats.Avg == 0 {
				continue
			}
			avg30d := stats.Avg30d
			if avg30d == 0 {
				avg30d = stats.Avg
			}
			pctDiff := math.Abs(stats.Avg-avg30d) / stats.Avg * 100
			return stats.Avg, pctDiff
		}
	}
	return 0, 100
}

func getFreshnessDays(pt PTCard) int {
	if pt.Updated == "" {
		return 999
	}
	t, err := time.Parse(time.RFC3339, pt.Updated)
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05.000Z", pt.Updated)
		if err != nil {
			return 999
		}
	}
	days := int(time.Since(t).Hours() / 24)
	if days < 0 {
		days = 0
	}
	return days
}

// ── CSV export ──

const csvPath = ".cache/liquidity_scored.csv"

func saveCSV(results []ScoredCard) {
	f, err := os.Create(csvPath)
	if err != nil {
		fmt.Printf("ERROR: Cannot create CSV: %v\n", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"tcgdex_id", "poketrace_id", "name", "set", "tcgdex_price", "poketrace_avg_nm",
		"total_sale_count", "ebay_sale_count", "tcgplayer_sale_count", "platform_count",
		"price_stability", "freshness_days", "liquidity_score", "matched",
		"set_slug", "card_number", "variant", "rarity", "image_url",
		"lp_avg_price", "nm_sale_count", "nm_avg7d", "nm_avg30d",
		"price_trend", "pack_score", "has_graded",
		"psa_10_avg", "psa_10_sale_count", "psa_10_avg7d", "psa_10_avg30d",
		"psa_9_avg", "psa_9_sale_count", "psa_9_avg7d", "psa_9_avg30d",
		"prev_sale_count", "prev_scan_date", "sales_per_week",
		"dedup_reason",
	}
	w.Write(header)

	for _, c := range results {
		variant := ""
		if c.Variant != nil {
			variant = *c.Variant
		}
		prevSales := ""
		if c.PrevSaleCount != nil {
			prevSales = strconv.Itoa(*c.PrevSaleCount)
		}
		prevDate := ""
		if c.PrevScanDate != nil {
			prevDate = *c.PrevScanDate
		}
		velocity := ""
		if c.SalesPerWeek != nil {
			velocity = fmt.Sprintf("%.1f", *c.SalesPerWeek)
		}
		dedup := ""
		if c.DedupReason != nil {
			dedup = *c.DedupReason
		}

		row := []string{
			c.TcgdexID, c.PoketraceID, c.Name, c.Set,
			fmt.Sprintf("%.2f", c.TcgdexPrice), fmt.Sprintf("%.2f", c.PoketraceAvgNM),
			strconv.Itoa(c.TotalSaleCount), strconv.Itoa(c.EbaySaleCount), strconv.Itoa(c.TcgplayerSaleCount), strconv.Itoa(c.PlatformCount),
			fmt.Sprintf("%.1f", c.PriceStability), strconv.Itoa(c.FreshnessDays), fmt.Sprintf("%.1f", c.LiquidityScore), strconv.FormatBool(c.Matched),
			c.SetSlug, c.CardNumber, variant, c.Rarity, c.ImageURL,
			fmt.Sprintf("%.2f", c.LPAvgPrice), strconv.Itoa(c.NMSaleCount), fmt.Sprintf("%.3f", c.NMAvg7d), fmt.Sprintf("%.3f", c.NMAvg30d),
			fmt.Sprintf("%.2f", c.PriceTrend), fmt.Sprintf("%.1f", c.PackScore), strconv.FormatBool(c.HasGraded),
			fmt.Sprintf("%.2f", c.PSA10Avg), strconv.Itoa(c.PSA10SaleCount), fmt.Sprintf("%.3f", c.PSA10Avg7d), fmt.Sprintf("%.3f", c.PSA10Avg30d),
			fmt.Sprintf("%.2f", c.PSA9Avg), strconv.Itoa(c.PSA9SaleCount), fmt.Sprintf("%.3f", c.PSA9Avg7d), fmt.Sprintf("%.3f", c.PSA9Avg30d),
			prevSales, prevDate, velocity,
			dedup,
		}
		w.Write(row)
	}

	fmt.Printf("  CSV saved to %s (%d rows)\n", csvPath, len(results))
}

// ── Summary ──

func printSummary(results []ScoredCard, totalInput int, prev prevScanMap) {
	matched := 0
	unmatched := 0
	for _, r := range results {
		if r.Matched {
			matched++
		} else {
			unmatched++
		}
	}

	fmt.Println("\n===== POKETRACE LIQUIDITY ENRICHMENT =====")
	fmt.Printf("  Cards in input:      %d\n", totalInput)
	fmt.Printf("  Processed so far:    %d\n", len(results))
	fmt.Printf("  Matched:             %d\n", matched)
	fmt.Printf("  Unmatched:           %d\n", unmatched)
	fmt.Printf("  Remaining:           %d\n", totalInput-len(results))
	remain := atomic.LoadInt64(&rateLimitRemain)
	if remain >= 0 {
		fmt.Printf("  API calls remaining: %d\n", remain)
	} else {
		fmt.Printf("  API calls remaining: unknown\n")
	}

	// Top 25 most liquid
	sortedLiq := make([]ScoredCard, 0)
	for _, r := range results {
		if r.Matched {
			sortedLiq = append(sortedLiq, r)
		}
	}
	sort.Slice(sortedLiq, func(i, j int) bool {
		return sortedLiq[i].LiquidityScore > sortedLiq[j].LiquidityScore
	})

	limit := 25
	if len(sortedLiq) < limit {
		limit = len(sortedLiq)
	}
	if limit > 0 {
		fmt.Println("\n  Top 25 Most Liquid (so far):")
		for i := 0; i < limit; i++ {
			c := sortedLiq[i]
			setStr := c.Set
			if setStr == "" {
				setStr = "Unknown Set"
			}
			fmt.Printf("    %2d. %-35s $%-8.2f  sales:%-5d  score:%.1f\n",
				i+1, fmt.Sprintf("%s (%s)", c.Name, setStr), c.TcgdexPrice, c.TotalSaleCount, c.LiquidityScore)
		}
	}

	// Top 25 velocity
	if prev != nil && len(prev) > 0 {
		var withVelocity []ScoredCard
		for _, r := range results {
			if r.Matched && r.SalesPerWeek != nil {
				withVelocity = append(withVelocity, r)
			}
		}
		sort.Slice(withVelocity, func(i, j int) bool {
			return *withVelocity[i].SalesPerWeek > *withVelocity[j].SalesPerWeek
		})

		velLimit := 25
		if len(withVelocity) < velLimit {
			velLimit = len(withVelocity)
		}
		if velLimit > 0 {
			fmt.Println("\n  Top 25 Highest Sales Velocity (cards/week):")
			for i := 0; i < velLimit; i++ {
				c := withVelocity[i]
				setStr := c.Set
				if setStr == "" {
					setStr = "Unknown Set"
				}
				fmt.Printf("    %2d. %-35s $%-8.2f  velocity:%.0f/week\n",
					i+1, fmt.Sprintf("%s (%s)", c.Name, setStr), c.TcgdexPrice, *c.SalesPerWeek)
			}
			fmt.Printf("\n  (Velocity data available for %d / %d cards — requires 2+ scans)\n",
				len(withVelocity), matched)
		} else {
			fmt.Println("\n  Sales Velocity: No velocity data computed yet — cards may not have changed since last scan.")
		}
	} else {
		fmt.Println("\n  Sales Velocity: No previous scan found — run again after the next scan cycle to calculate velocity.")
	}

	// Score distribution (0-100 normalized)
	buckets := map[string]int{
		"0-20":  0,
		"20-40": 0,
		"40-60": 0,
		"60-80": 0,
		"80+":   0,
	}
	for _, r := range results {
		if !r.Matched {
			continue
		}
		switch {
		case r.LiquidityScore < 20:
			buckets["0-20"]++
		case r.LiquidityScore < 40:
			buckets["20-40"]++
		case r.LiquidityScore < 60:
			buckets["40-60"]++
		case r.LiquidityScore < 80:
			buckets["60-80"]++
		default:
			buckets["80+"]++
		}
	}

	fmt.Println("\n  Liquidity Score Distribution (0-100 normalized):")
	for _, label := range []string{"0-20", "20-40", "40-60", "60-80", "80+"} {
		fmt.Printf("    %-10s %d cards\n", label+":", buckets[label])
	}

	fmt.Printf("\n  Output saved to %s\n", outputPath)
	fmt.Println("==========================================")
}
