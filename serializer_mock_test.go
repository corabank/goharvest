package goharvest

import "github.com/obsidiandynamics/libstdgo/concurrent"

type serMockFuncs struct {
	Serialize func(m *serMock, topic string, data []byte) ([]byte, error)
}

type serMockCounts struct {
	Serialize concurrent.AtomicCounter
}

type serMock struct {
	f serMockFuncs
	c serMockCounts
}

func (m *serMock) Serialize(topic string, data []byte) ([]byte, error) {
	defer m.c.Serialize.Inc()
	return m.f.Serialize(m, topic, data)
}

func (m *serMock) fillDefaults() {
	if m.f.Serialize == nil {
		m.f.Serialize = func(m *serMock, topic string, data []byte) ([]byte, error) {
			return data, nil
		}
	}

	m.c.Serialize = concurrent.NewAtomicCounter()
}
