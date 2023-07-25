package store

import "fmt"

type Service struct{}

func NewService() *Service {
	return &Service{}
}

func (s *Service) ServeMapData() error {
	return fmt.Errorf("No implemented")
}
