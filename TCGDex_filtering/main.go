package main

import (
	"encoding/json"
	"fmt"
	"io"
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
// Output card type
// ---------------------------------------------------------------------------

type Card struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
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
// Cache
// ---------------------------------------------------------------------------

const cacheDir = ".cache"

func cachePath() string { return filepath.Join(cacheDir, "cards.json") }

func loadCache() ([]Card, bool) {
	data, err := os.ReadFile(cachePath())
	if err != nil {
		return nil, false
	}
	var cards []Card
	if err := json.Unmarshal(data, &cards); err != nil || len(cards) == 0 {
		return nil, false
	}
	return cards, true
}

func saveCache(cards []Card) {
	os.MkdirAll(cacheDir, 0o755)
	data, _ := json.MarshalIndent(cards, "", "  ")
	os.WriteFile(cachePath(), data, 0o644)
}

// ---------------------------------------------------------------------------
// Fetch all cards
// ---------------------------------------------------------------------------

func fetchAllCards() []Card {
	// Try cache
	if cards, ok := loadCache(); ok {
		fmt.Printf("[cache] Loaded %d cards from %s\n", len(cards), cachePath())
		return cards
	}

	start := time.Now()

	// Step 1: Fetch index
	fmt.Println("Step 1: Fetching card index from https://api.tcgdex.net/v2/en/cards ...")
	var index []CardIndexEntry
	if err := fetchJSON("https://api.tcgdex.net/v2/en/cards", &index); err != nil {
		fmt.Printf("ERROR: %v\n", err)
		os.Exit(1)
	}
	total := len(index)
	fmt.Printf("  Found %d cards in index\n\n", total)

	// Step 2: Fetch details concurrently
	fmt.Printf("Step 2: Fetching price data for all %d cards (50 concurrent workers)...\n", total)

	type result struct {
		card Card
		ok   bool
	}
	results := make([]result, total)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 50)
	var mu sync.Mutex
	var done int64
	var withPrice, withoutPrice int64

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
				withoutPrice++
				mu.Unlock()
				return
			}
			price := detail.bestPrice()
			mu.Lock()
			done++
			if price > 0 {
				withPrice++
				results[idx] = result{
					card: Card{ID: e.ID, Name: e.Name, Price: price},
					ok:   true,
				}
			} else {
				withoutPrice++
			}
			d := done
			mu.Unlock()
			if d%500 == 0 || d == int64(total) {
				pct := float64(d) / float64(total) * 100
				fmt.Printf("\r  Progress: %d / %d (%.1f%%)", d, total, pct)
			}
		}(i, entry)
	}
	wg.Wait()
	fmt.Println()

	// Collect and sort
	var cards []Card
	for _, r := range results {
		if r.ok {
			cards = append(cards, r.card)
		}
	}
	sort.Slice(cards, func(i, j int) bool { return cards[i].Price < cards[j].Price })

	elapsed := time.Since(start)

	// Save cache
	saveCache(cards)

	// Print summary
	fmt.Printf("\n  Fetch completed in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Total cards in index:    %d\n", total)
	fmt.Printf("  Cards with price data:   %d\n", len(cards))
	fmt.Printf("  Cards without price data: %d (excluded)\n", total-len(cards))

	return cards
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

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

func printStats(cards []Card) {
	n := len(cards)
	if n == 0 {
		fmt.Println("No cards to analyze.")
		return
	}

	prices := make([]float64, n)
	var sum float64
	for i, c := range cards {
		prices[i] = c.Price
		sum += c.Price
	}
	// Already sorted from fetch
	sort.Float64s(prices)

	fmt.Println("\n========== CARD PRICE SUMMARY ==========")
	fmt.Printf("  Total priced cards: %d\n", n)
	fmt.Printf("  Min price:    $%.2f\n", prices[0])
	fmt.Printf("  Max price:    $%.2f\n", prices[n-1])
	fmt.Printf("  Median price: $%.2f\n", median(prices))
	fmt.Printf("  Mean price:   $%.2f\n", sum/float64(n))

	// Percentiles
	p := func(pct float64) float64 {
		idx := int(float64(n-1) * pct / 100)
		return prices[idx]
	}
	fmt.Printf("  25th pctl:    $%.2f\n", p(25))
	fmt.Printf("  75th pctl:    $%.2f\n", p(75))
	fmt.Printf("  90th pctl:    $%.2f\n", p(90))
	fmt.Printf("  95th pctl:    $%.2f\n", p(95))
	fmt.Printf("  99th pctl:    $%.2f\n", p(99))

	// Price distribution buckets
	brackets := []struct {
		label string
		lo    float64
		hi    float64
	}{
		{"Under $0.50", 0, 0.50},
		{"$0.50 – $1", 0.50, 1},
		{"$1 – $2", 1, 2},
		{"$2 – $5", 2, 5},
		{"$5 – $10", 5, 10},
		{"$10 – $25", 10, 25},
		{"$25 – $50", 25, 50},
		{"$50 – $100", 50, 100},
		{"$100 – $500", 100, 500},
		{"$500+", 500, 1e9},
	}
	fmt.Println("\n  Price Distribution:")
	for _, br := range brackets {
		count := 0
		for _, pr := range prices {
			if pr >= br.lo && pr < br.hi {
				count++
			}
		}
		pct := float64(count) / float64(n) * 100
		bar := ""
		barLen := int(pct / 2)
		for i := 0; i < barLen; i++ {
			bar += "#"
		}
		fmt.Printf("    %-14s %6d  (%5.1f%%)  %s\n", br.label, count, pct, bar)
	}

	// Top 10 most expensive
	fmt.Println("\n  Top 10 Most Expensive:")
	start := n - 10
	if start < 0 {
		start = 0
	}
	for i := n - 1; i >= start; i-- {
		fmt.Printf("    %2d. %-40s $%.2f\n", n-i, cards[i].Name, cards[i].Price)
	}

	fmt.Println("=========================================")
	fmt.Printf("\nCached to %s\n", cachePath())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	allCards := fetchAllCards()
	fmt.Println("\n===== ALL CARDS (any price) =====")
	printStats(allCards)

	// Filter to $1+ only
	var filtered []Card
	for _, c := range allCards {
		if c.Price >= 1.0 {
			filtered = append(filtered, c)
		}
	}

	excluded := len(allCards) - len(filtered)
	fmt.Printf("\n===== FILTERED: $1.00+ ONLY =====")
	fmt.Printf("\n  Excluded %d cards under $1.00 (%.1f%% of total)\n",
		excluded, float64(excluded)/float64(len(allCards))*100)
	fmt.Printf("  Kept %d cards (%.1f%% of total)\n",
		len(filtered), float64(len(filtered))/float64(len(allCards))*100)
	printStats(filtered)

	// Overwrite cache with filtered cards only
	saveCache(filtered)
	fmt.Printf("\n  Cache updated: %d cards saved to %s\n", len(filtered), cachePath())

	fmt.Println("Done.")
}
