package main

import (
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
	Variant    string            `json:"variant"`
	Rarity     string            `json:"rarity"`
	Market     string            `json:"market"`
	Currency   string            `json:"currency"`
	Prices     map[string]PTTier `json:"prices"` // "ebay", "tcgplayer" → condition → stats
	Updated    string            `json:"lastUpdated"`
}

type PTSet struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

// PTTier maps condition names (NEAR_MINT, LIGHTLY_PLAYED, etc.) to stats.
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

// ── Output types ──

type ScoredCard struct {
	TcgdexID          string  `json:"tcgdex_id"`
	PoketraceID       string  `json:"poketrace_id"`
	Name              string  `json:"name"`
	Set               string  `json:"set"`
	TcgdexPrice       float64 `json:"tcgdex_price"`
	PoketraceAvgNM    float64 `json:"poketrace_avg_nm"`
	TotalSaleCount    int     `json:"total_sale_count"`
	EbaySaleCount     int     `json:"ebay_sale_count"`
	TcgplayerSaleCount int    `json:"tcgplayer_sale_count"`
	PlatformCount     int     `json:"platform_count"`
	PriceStability    float64 `json:"price_stability"`
	FreshnessDays     int     `json:"freshness_days"`
	LiquidityScore    float64 `json:"liquidity_score"`
	Matched           bool    `json:"matched"`
}

type ProgressFile struct {
	ProcessedIDs []string    `json:"processed_ids"`
	Results      []ScoredCard `json:"results"`
	LastIndex    int         `json:"last_index"`
}

// ── Paths ──

const (
	envPath          = "../.env"
	tcgdexCachePath  = "../TCGDex_filtering/.cache/cards.json"
	progressPath     = ".cache/liquidity_progress.json"
	outputPath       = ".cache/liquidity_scored.json"
	baseURL          = "https://api.poketrace.com/v1"
	requestDelay     = 2500 * time.Millisecond
	checkpointEvery  = 50
)

var (
	apiKey           string
	rateLimitRemain  = -1 // unknown until first response
)

func main() {
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

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	stopping := false

	// Process cards
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

	for i, card := range filtered {
		// Check for shutdown signal (non-blocking)
		select {
		case <-sigChan:
			fmt.Println("\n  Caught interrupt, saving progress...")
			stopping = true
		default:
		}
		if stopping {
			break
		}

		// Skip already processed
		if processedSet[card.ID] {
			continue
		}

		// Check rate limit
		if rateLimitRemain >= 0 && rateLimitRemain <= 5 {
			fmt.Printf("\n  Rate limit nearly exhausted (%d remaining), stopping gracefully.\n", rateLimitRemain)
			break
		}

		// Throttle
		if processed > len(progress.Results) {
			// Only delay after the first new request
			time.Sleep(requestDelay)
		}

		// Search PokeTrace
		scored, err := enrichCard(card)
		if err != nil {
			fmt.Printf("  [%d/%d] ERROR %s: %v\n", i+1, len(filtered), card.Name, err)
			// On fatal error (not retryable), mark unmatched
			scored = unmatchedCard(card)
		}

		results = append(results, scored)
		processedSet[card.ID] = true
		processed++

		if scored.Matched {
			matched++
			fmt.Printf("  [%d/%d] ✓ %s — sales:%d score:%.1f\n",
				processed, len(filtered), card.Name, scored.TotalSaleCount, scored.LiquidityScore)
		} else {
			unmatched++
			fmt.Printf("  [%d/%d] ✗ %s — unmatched\n", processed, len(filtered), card.Name)
		}

		// Checkpoint
		if processed%checkpointEvery == 0 {
			saveProgress(results, processedSet)
			fmt.Printf("  Checkpoint saved (%d cards)\n", processed)
		}
	}

	// Final save
	saveProgress(results, processedSet)
	saveOutput(results)
	printSummary(results, len(filtered))
}

// ── .env + API key loading ──

func loadAPIKey() string {
	// Try .env file first
	data, err := os.ReadFile(envPath)
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "POKETRACE_API_KEY=") {
				return strings.TrimPrefix(line, "POKETRACE_API_KEY=")
			}
		}
	}
	// Fallback to env var
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
	// Sort by liquidity score descending
	sorted := make([]ScoredCard, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LiquidityScore > sorted[j].LiquidityScore
	})
	data, _ := json.MarshalIndent(sorted, "", "  ")
	os.WriteFile(outputPath, data, 0644)
}

// ── PokeTrace API ──

func enrichCard(card TCGDexCard) (ScoredCard, error) {
	ptCards, err := searchPokeTrace(card.Name)
	if err != nil {
		return ScoredCard{}, err
	}

	if len(ptCards) == 0 {
		return unmatchedCard(card), nil
	}

	// Find best match
	best := findBestMatch(card, ptCards)
	if best == nil {
		return unmatchedCard(card), nil
	}

	return scoreCard(card, *best), nil
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

		// Track rate limit
		if rl := resp.Header.Get("X-RateLimit-Remaining"); rl != "" {
			if val, err := strconv.Atoi(rl); err == nil {
				rateLimitRemain = val
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
	// Extract set ID from TCGDex ID (e.g., "base4-45" → "base4")
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

		// 1. Name match (case-insensitive)
		if strings.EqualFold(pt.Name, tcgCard.Name) {
			s += 100
		} else if strings.Contains(strings.ToLower(pt.Name), strings.ToLower(tcgCard.Name)) {
			s += 50
		} else {
			// No name match at all — skip
			continue
		}

		// 2. Set similarity (fuzzy)
		if tcgSetID != "" {
			ptSetNorm := normalizeSetName(pt.Set.Name)
			tcgSetNorm := normalizeSetName(tcgSetID)
			if strings.Contains(ptSetNorm, tcgSetNorm) || strings.Contains(tcgSetNorm, ptSetNorm) {
				s += 50
			} else if fuzzySetMatch(pt.Set.Name, tcgSetID) {
				s += 25
			}
		}

		// 3. Total sale count as tiebreaker value
		totalSales := countTotalSales(pt)

		candidates = append(candidates, scored{card: pt, score: s, sales: totalSales})
	}

	if len(candidates) == 0 {
		return nil
	}

	// Sort: highest score first, then highest sales
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

	// Check if significant words overlap
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

// ── Scoring ──

func scoreCard(tcg TCGDexCard, pt PTCard) ScoredCard {
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

	// Formula
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

	liquidityScore := (float64(totalSales) * 0.5) +
		(platformBonus * 0.2) +
		(stabilityScore * 0.2) +
		(freshnessScore * 0.1)

	// Round to 1 decimal
	liquidityScore = math.Round(liquidityScore*10) / 10
	stability = math.Round(stability*10) / 10

	return ScoredCard{
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
		LiquidityScore:     liquidityScore,
		Matched:            true,
	}
}

func unmatchedCard(tcg TCGDexCard) ScoredCard {
	return ScoredCard{
		TcgdexID:    tcg.ID,
		Name:        tcg.Name,
		TcgdexPrice: tcg.Price,
		Matched:     false,
	}
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
	// Try NEAR_MINT first, then fallback to highest available condition
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
	return 0, 100 // no data → worst stability
}

func getFreshnessDays(pt PTCard) int {
	if pt.Updated == "" {
		return 999
	}
	t, err := time.Parse(time.RFC3339, pt.Updated)
	if err != nil {
		// Try without timezone
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

// ── Summary ──

func printSummary(results []ScoredCard, totalInput int) {
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
	if rateLimitRemain >= 0 {
		fmt.Printf("  API calls remaining: %d\n", rateLimitRemain)
	} else {
		fmt.Printf("  API calls remaining: unknown\n")
	}

	// Top 25 most liquid
	sorted := make([]ScoredCard, 0)
	for _, r := range results {
		if r.Matched {
			sorted = append(sorted, r)
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LiquidityScore > sorted[j].LiquidityScore
	})

	limit := 25
	if len(sorted) < limit {
		limit = len(sorted)
	}
	if limit > 0 {
		fmt.Println("\n  Top 25 Most Liquid (so far):")
		for i := 0; i < limit; i++ {
			c := sorted[i]
			setStr := c.Set
			if setStr == "" {
				setStr = "Unknown Set"
			}
			fmt.Printf("    %2d. %-35s $%-8.2f  sales:%-5d  score:%.1f\n",
				i+1, fmt.Sprintf("%s (%s)", c.Name, setStr), c.TcgdexPrice, c.TotalSaleCount, c.LiquidityScore)
		}
	}

	// Score distribution
	buckets := map[string]int{
		"0-50":    0,
		"50-100":  0,
		"100-250": 0,
		"250-500": 0,
		"500+":    0,
	}
	for _, r := range results {
		if !r.Matched {
			continue
		}
		switch {
		case r.LiquidityScore < 50:
			buckets["0-50"]++
		case r.LiquidityScore < 100:
			buckets["50-100"]++
		case r.LiquidityScore < 250:
			buckets["100-250"]++
		case r.LiquidityScore < 500:
			buckets["250-500"]++
		default:
			buckets["500+"]++
		}
	}

	fmt.Println("\n  Liquidity Score Distribution:")
	for _, label := range []string{"0-50", "50-100", "100-250", "250-500", "500+"} {
		fmt.Printf("    %-10s %d cards\n", label+":", buckets[label])
	}

	fmt.Printf("\n  Output saved to %s\n", outputPath)
	fmt.Println("==========================================")
}
