package datagram

// Quality mirrors OPC-UA's coarse status classification (DGM-100).
type Quality string

const (
	QualityGood      Quality = "GOOD"
	QualityUncertain Quality = "UNCERTAIN"
	QualityStale     Quality = "STALE"
	QualityBad       Quality = "BAD"
)

// rank orders qualities from best to worst so Combine can pick the worst.
var rank = map[Quality]int{
	QualityGood:      0,
	QualityUncertain: 1,
	QualityStale:     2,
	QualityBad:       3,
}

// Combine derives the worst of the given qualities (DGM-140: "processors
// combining multiple inputs derive the worst input quality by default").
// An empty input returns QualityGood, matching the default single-input case.
func Combine(qualities ...Quality) Quality {
	worst := QualityGood
	for _, q := range qualities {
		if rank[q] > rank[worst] {
			worst = q
		}
	}
	return worst
}
