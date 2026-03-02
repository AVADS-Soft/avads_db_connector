package avads_db_connector

// NewConnection создание нового подключения
func NewConnection(addr string, port string, login string, pass string) *ConnectionT {
	var retConn = ConnectionT{
		Address:      addr,
		Port:         port,
		Login:        login,
		Pass:         pass,
		stream:       LteDBInOutTCP{},
		TryBaseList:  map[int]string{},
		OpenBaseList: map[int]string{},
	}
	return &retConn
}
