package v1

func (s *Server) Emit(evt *CDCEvent) {
	s.events <- evt
}
