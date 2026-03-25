package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ── Input type ──

type InputCard struct {
	TcgplayerID string `json:"tcgplayer_id"`
	Name        string `json:"name"`
}

// ── Product Details (mp-search-api) — every field ──

type ProductDetails struct {
	CustomListings           float64                `json:"customListings"`
	ShippingCategoryID       float64                `json:"shippingCategoryId"`
	Duplicate                bool                   `json:"duplicate"`
	ProductLineUrlName       string                 `json:"productLineUrlName"`
	ProductTypeName          string                 `json:"productTypeName"`
	ProductUrlName           string                 `json:"productUrlName"`
	ProductTypeID            float64                `json:"productTypeId"`
	RarityName               string                 `json:"rarityName"`
	Sealed                   bool                   `json:"sealed"`
	MarketPrice              float64                `json:"marketPrice"`
	CustomAttributes         map[string]interface{} `json:"customAttributes"`
	LowestPriceWithShipping  float64                `json:"lowestPriceWithShipping"`
	ProductName              string                 `json:"productName"`
	SetID                    float64                `json:"setId"`
	SetCode                  string                 `json:"setCode"`
	ProductID                float64                `json:"productId"`
	ImageCount               float64                `json:"imageCount"`
	Score                    float64                `json:"score"`
	SetName                  string                 `json:"setName"`
	Sellers                  float64                `json:"sellers"`
	FoilOnly                 bool                   `json:"foilOnly"`
	SetUrlName               string                 `json:"setUrlName"`
	SellerListable           bool                   `json:"sellerListable"`
	ProductLineID            float64                `json:"productLineId"`
	ProductStatusID          float64                `json:"productStatusId"`
	ProductLineName          string                 `json:"productLineName"`
	MaxFulfillableQuantity   float64                `json:"maxFulfillableQuantity"`
	NormalOnly               bool                   `json:"normalOnly"`
	Listings                 float64                `json:"listings"`
	LowestPrice              float64                `json:"lowestPrice"`
	MedianPrice              *float64               `json:"medianPrice"`
	FormattedAttributes      map[string]string      `json:"formattedAttributes"`
	Skus                     []SKU                  `json:"skus"`
}

type SKU struct {
	Sku       int    `json:"sku"`
	Condition string `json:"condition"`
	Variant   string `json:"variant"`
	Language  string `json:"language"`
}

// ── Price History (infinite-api) — every field ──

type PriceHistoryResponse struct {
	Count  int                `json:"count"`
	Result []PriceHistoryItem `json:"result"`
}

type PriceHistoryItem struct {
	SkuID                            string                 `json:"skuId"`
	Variant                          string                 `json:"variant"`
	Language                         string                 `json:"language"`
	Condition                        string                 `json:"condition"`
	AverageDailyQuantitySold         string                 `json:"averageDailyQuantitySold"`
	AverageDailyTransactionCount     string                 `json:"averageDailyTransactionCount"`
	TotalQuantitySold                string                 `json:"totalQuantitySold"`
	TotalTransactionCount            string                 `json:"totalTransactionCount"`
	TrendingMarketPricePercentages   map[string]interface{} `json:"trendingMarketPricePercentages"`
	Buckets                          []PriceBucket          `json:"buckets"`
}

type PriceBucket struct {
	MarketPrice              string `json:"marketPrice"`
	QuantitySold             string `json:"quantitySold"`
	LowSalePrice             string `json:"lowSalePrice"`
	LowSalePriceWithShipping string `json:"lowSalePriceWithShipping"`
	HighSalePrice            string `json:"highSalePrice"`
	HighSalePriceWithShipping string `json:"highSalePriceWithShipping"`
	TransactionCount         string `json:"transactionCount"`
	BucketStartDate          string `json:"bucketStartDate"`
}

// ── Combined output per card ──

type ScrapedCard struct {
	TcgplayerID    string                `json:"tcgplayer_id"`
	Name           string                `json:"name"`
	ScrapedAt      string                `json:"scraped_at"`
	ProductDetails *ProductDetails       `json:"product_details"`
	PriceHistory   *PriceHistoryResponse `json:"price_history"`
	Errors         []string              `json:"errors,omitempty"`
}

type DetailsEntry struct {
	TcgplayerID    string          `json:"tcgplayer_id"`
	Name           string          `json:"name"`
	ScrapedAt      string          `json:"scraped_at"`
	ProductDetails *ProductDetails `json:"product_details"`
}

type PriceHistoryEntry struct {
	TcgplayerID  string                `json:"tcgplayer_id"`
	Name         string                `json:"name"`
	ScrapedAt    string                `json:"scraped_at"`
	PriceHistory *PriceHistoryResponse `json:"price_history"`
}

type DetailsOutputFile struct {
	ScanTimestamp string         `json:"scan_timestamp"`
	TotalCards    int            `json:"total_cards"`
	Cards         []DetailsEntry `json:"cards"`
}

type PriceHistoryOutputFile struct {
	ScanTimestamp string               `json:"scan_timestamp"`
	TotalCards    int                  `json:"total_cards"`
	Cards         []PriceHistoryEntry  `json:"cards"`
}

type ProgressFile struct {
	ProcessedIDs []string      `json:"processed_ids"`
	Results      []ScrapedCard `json:"results"`
}

// ── Config ──

const (
	inputPath       = "../poketrace_liquidity/.cache/tcgplayer_ids.json"
	progressPath    = ".cache/tcgplayer_progress.json"
	detailsOutputPath      = ".cache/tcgplayer_details.json"
	priceHistoryOutputPath = ".cache/tcgplayer_price_history.json"
	detailsURL      = "https://mp-search-api.tcgplayer.com/v2/product/%s/details?mpfev=4952"
	priceHistoryURL = "https://infinite-api.tcgplayer.com/price/history/%s/detailed?range=quarter"
	numWorkers      = 3
	requestDelay    = 1000 * time.Millisecond // 1 req/sec per worker, ~3 req/sec total
	checkpointEvery = 25
)

var (
	stopFlag int64
	client   = &http.Client{Timeout: 30 * time.Second}
)

func main() {
	startTime := time.Now()

	// Load input
	cards := loadInput()
	fmt.Printf("Loaded %d cards from %s\n", len(cards), inputPath)

	// Load progress
	progress := loadProgress()
	processedSet := make(map[string]bool)
	for _, id := range progress.ProcessedIDs {
		processedSet[id] = true
	}
	results := progress.Results
	if len(results) > 0 {
		fmt.Printf("Resuming: %d cards already processed\n", len(results))
	}

	// Build work queue
	var toProcess []InputCard
	for _, card := range cards {
		if !processedSet[card.TcgplayerID] {
			toProcess = append(toProcess, card)
		}
	}
	fmt.Printf("Cards to process: %d (with %d workers)\n", len(toProcess), numWorkers)

	if len(toProcess) == 0 {
		fmt.Println("Nothing to process.")
		saveOutput(results)
		printSummary(results)
		return
	}

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n  Caught interrupt, finishing in-flight requests...")
		atomic.StoreInt64(&stopFlag, 1)
	}()

	// Rate limiter: 1 token every 500ms
	rateLimiter := time.NewTicker(requestDelay)
	defer rateLimiter.Stop()

	// Channels
	jobs := make(chan InputCard, len(toProcess))
	resultsChan := make(chan ScrapedCard, len(toProcess))

	// Feed jobs
	go func() {
		for _, card := range toProcess {
			if atomic.LoadInt64(&stopFlag) == 1 {
				break
			}
			jobs <- card
		}
		close(jobs)
	}()

	// Launch workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for card := range jobs {
				if atomic.LoadInt64(&stopFlag) == 1 {
					return
				}

				// Rate limit: wait for token before details call
				<-rateLimiter.C
				scraped := scrapeCard(card, rateLimiter)
				resultsChan <- scraped
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	mu := &sync.Mutex{}
	processed := len(results)
	success := 0
	fail := 0
	for _, r := range results {
		if len(r.Errors) == 0 {
			success++
		} else {
			fail++
		}
	}

	for res := range resultsChan {
		mu.Lock()
		results = append(results, res)
		processedSet[res.TcgplayerID] = true
		processed++

		if len(res.Errors) == 0 {
			success++
			fmt.Printf("  [%d/%d] ✓ %s (id:%s)\n", processed, len(cards), res.Name, res.TcgplayerID)
		} else {
			fail++
			fmt.Printf("  [%d/%d] ✗ %s — %v\n", processed, len(cards), res.Name, res.Errors)
		}

		if processed%checkpointEvery == 0 {
			saveProgress(results, processedSet)
			fmt.Printf("  Checkpoint saved (%d cards)\n", processed)
		}
		mu.Unlock()
	}

	// Final save
	saveProgress(results, processedSet)
	saveOutput(results)
	printSummary(results)

	elapsed := time.Since(startTime)
	fmt.Printf("\n  Completed in %s\n", elapsed.Round(time.Second))
}

// ── Scraping ──

func scrapeCard(card InputCard, rateLimiter *time.Ticker) ScrapedCard {
	sc := ScrapedCard{
		TcgplayerID: card.TcgplayerID,
		Name:        card.Name,
		ScrapedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	// 1. Product details
	details, err := fetchDetails(card.TcgplayerID)
	if err != nil {
		sc.Errors = append(sc.Errors, fmt.Sprintf("details: %v", err))
	} else {
		sc.ProductDetails = details
	}

	// Rate limit before second call
	<-rateLimiter.C

	// 2. Price history
	history, err := fetchPriceHistory(card.TcgplayerID)
	if err != nil {
		sc.Errors = append(sc.Errors, fmt.Sprintf("price_history: %v", err))
	} else {
		sc.PriceHistory = history
	}

	return sc
}

func fetchDetails(tcgplayerID string) (*ProductDetails, error) {
	url := fmt.Sprintf(detailsURL, tcgplayerID)
	return fetchWithRetry[ProductDetails](url)
}

func fetchPriceHistory(tcgplayerID string) (*PriceHistoryResponse, error) {
	url := fmt.Sprintf(priceHistoryURL, tcgplayerID)
	return fetchWithRetry[PriceHistoryResponse](url)
}

func fetchWithRetry[T any](endpoint string) (*T, error) {
	backoffs := []time.Duration{10 * time.Second, 30 * time.Second, 60 * time.Second}

	var lastErr error
	for attempt := 0; attempt <= 3; attempt++ {
		if attempt > 0 {
			fmt.Printf("    Retry %d/3 after %s...\n", attempt, backoffs[attempt-1])
			time.Sleep(backoffs[attempt-1])
		}

		req, _ := http.NewRequest("GET", endpoint, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
		req.Header.Set("Origin", "https://www.tcgplayer.com")
		req.Header.Set("Referer", "https://www.tcgplayer.com/")
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("HTTP error: %w", err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 429 || resp.StatusCode == 403 || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			continue
		}

		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body[:min(200, len(body))]))
		}

		if err != nil {
			return nil, fmt.Errorf("read body: %w", err)
		}

		var result T
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, fmt.Errorf("JSON decode: %w", err)
		}

		return &result, nil
	}

	return nil, fmt.Errorf("all retries exhausted: %w", lastErr)
}

// ── I/O ──

func loadInput() []InputCard {
	data, err := os.ReadFile(inputPath)
	if err != nil {
		fmt.Printf("ERROR: Cannot read input at %s\n", inputPath)
		fmt.Println("  Run poketrace_liquidity first to generate tcgplayer_ids.json")
		os.Exit(1)
	}
	var cards []InputCard
	if err := json.Unmarshal(data, &cards); err != nil {
		fmt.Printf("ERROR: Invalid JSON in input: %v\n", err)
		os.Exit(1)
	}
	return cards
}

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

func saveProgress(results []ScrapedCard, processedSet map[string]bool) {
	ids := make([]string, 0, len(processedSet))
	for id := range processedSet {
		ids = append(ids, id)
	}
	p := ProgressFile{
		ProcessedIDs: ids,
		Results:      results,
	}
	data, _ := json.MarshalIndent(p, "", "  ")
	os.MkdirAll(".cache", 0755)
	os.WriteFile(progressPath, data, 0644)
}

func saveOutput(results []ScrapedCard) {
	now := time.Now().UTC().Format(time.RFC3339)

	// 1. Product details file
	var details []DetailsEntry
	for _, r := range results {
		if r.ProductDetails != nil {
			details = append(details, DetailsEntry{
				TcgplayerID:    r.TcgplayerID,
				Name:           r.Name,
				ScrapedAt:      r.ScrapedAt,
				ProductDetails: r.ProductDetails,
			})
		}
	}
	detailsOut := DetailsOutputFile{
		ScanTimestamp: now,
		TotalCards:    len(details),
		Cards:         details,
	}
	data, _ := json.MarshalIndent(detailsOut, "", "  ")
	os.WriteFile(detailsOutputPath, data, 0644)
	fmt.Printf("  Details saved to %s (%d cards)\n", detailsOutputPath, len(details))

	// 2. Price history file
	var history []PriceHistoryEntry
	for _, r := range results {
		if r.PriceHistory != nil {
			history = append(history, PriceHistoryEntry{
				TcgplayerID:  r.TcgplayerID,
				Name:         r.Name,
				ScrapedAt:    r.ScrapedAt,
				PriceHistory: r.PriceHistory,
			})
		}
	}
	historyOut := PriceHistoryOutputFile{
		ScanTimestamp: now,
		TotalCards:    len(history),
		Cards:         history,
	}
	data, _ = json.MarshalIndent(historyOut, "", "  ")
	os.WriteFile(priceHistoryOutputPath, data, 0644)
	fmt.Printf("  Price history saved to %s (%d cards)\n", priceHistoryOutputPath, len(history))
}

func printSummary(results []ScrapedCard) {
	success := 0
	fail := 0
	detailFail := 0
	historyFail := 0
	for _, r := range results {
		if len(r.Errors) == 0 {
			success++
		} else {
			fail++
			for _, e := range r.Errors {
				if len(e) > 7 && e[:7] == "details" {
					detailFail++
				}
				if len(e) > 13 && e[:13] == "price_history" {
					historyFail++
				}
			}
		}
	}

	fmt.Println("\n===== TCGPLAYER SCRAPING SUMMARY =====")
	fmt.Printf("  Total cards:       %d\n", len(results))
	fmt.Printf("  Successful:        %d\n", success)
	fmt.Printf("  Failed:            %d\n", fail)
	fmt.Printf("    Detail failures: %d\n", detailFail)
	fmt.Printf("    History failures:%d\n", historyFail)
	fmt.Printf("  Details:           %s\n", detailsOutputPath)
	fmt.Printf("  Price History:     %s\n", priceHistoryOutputPath)
	fmt.Println("======================================")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
