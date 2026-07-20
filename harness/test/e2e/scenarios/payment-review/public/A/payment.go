package payment

import (
	"errors"
	"fmt"
)

var ErrInvalidCharge = errors.New("invalid charge")

type Charge struct {
	ID    string
	Cents int64
}

type Processor struct {
	next    uint64
	charges map[string]Charge
}

func NewProcessor() *Processor {
	return &Processor{charges: make(map[string]Charge)}
}

func (p *Processor) Charge(idempotencyKey string, cents int64) (Charge, error) {
	if idempotencyKey == "" || cents <= 0 {
		return Charge{}, ErrInvalidCharge
	}
	if prior, ok := p.charges[idempotencyKey]; ok {
		if prior.Cents != cents {
			return Charge{}, ErrInvalidCharge
		}
		return prior, nil
	}
	p.next++
	charge := Charge{ID: fmt.Sprintf("ch_%06d", p.next), Cents: cents}
	p.charges[idempotencyKey] = charge
	return charge, nil
}

func (p *Processor) Count() int {
	return len(p.charges)
}
