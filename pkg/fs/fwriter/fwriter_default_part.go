package fwriter

type Part struct {
	buf  []byte
	woff int
}

func (p *Part) Len() int64 {
	return int64(p.woff)
}

func (p *Part) Cap() int64 {
	return int64(len(p.buf) - p.woff)
}

func (p *Part) Full() bool {
	return len(p.buf) == p.woff
}

func (p *Part) Write(data []byte) int {
	n := copy(p.buf[p.woff:], data)
	p.woff += n
	return n
}

func (p *Part) Copy(s *Part, off int) int64 {
	return int64(p.Write(s.buf[off:s.woff]))
}
