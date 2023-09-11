package cmdb

func HostToCluster(host string) string {
	cluster := "Default"
	switch host {
	case "brg01.bj":
		cluster = "Brg"
	case "brg02.bj":
		cluster = "Brg"
	case "freeze01.bj":
		cluster = "Freeze"
	default:
		cluster = "Default"
	}
	return cluster
}
