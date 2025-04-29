package reaper

type ReapParams struct {
}

// Reap will take in the params and throw it into the Garbage Collector.
func (r *Reaper) Reap(p *ReapParams) {
	r.collector <- *p
}

func (r *Reaper) write(p ReapParams) {

}
