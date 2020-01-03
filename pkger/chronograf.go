package pkger

import "github.com/influxdata/influxdb"

//func ConvertToCellView(c influxdb.Cell) chart {
//	return convertCellView(c)
//}

func DashboardToResource(dash influxdb.Dashboard, name string) Resource {
	return dashboardToResource(dash, name)
}
