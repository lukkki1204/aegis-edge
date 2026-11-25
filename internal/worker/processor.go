package worker

import (
	"fmt"

	v1 "github.com/twojanazwa/aegis-edge/api/proto/v1"
)

type Processor struct {
	repo *Repository
}

func NewProcessor(repo *Repository) *Processor {
	return &Processor{
		repo: repo,
	}
}

func (p *Processor) Process(msg *v1.DeviceMessage) error {
	if p == nil || p.repo == nil {
		return fmt.Errorf("processor or repository is nil")
	}
	return p.repo.SaveMessage(msg)
}
