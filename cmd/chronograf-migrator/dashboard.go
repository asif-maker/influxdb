package main

import (
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/pkger"
)

func Convert1To2Dashboard(d1 *chronograf.Dashboard) (pkger.Resource, error) {
	d2 := &influxdb.Dashboard{
		Name: d1.Name,
	}

	for _, cell := range d1.Cells {
		c := &influxdb.Cell{
			ID: 1,
			CellProperty: influxdb.CellProperty{
				X: cell.X,
				Y: cell.Y,
				W: cell.W,
				H: cell.H,
			},
		}

		v := influxdb.View{
			ViewContents: influxdb.ViewContents{
				Name: cell.Name,
			},
		}

		switch cell.Type {
		case "line":
			v.Properties = influxdb.XYViewProperties{
				Queries:    convertQueries(cell.Queries),
				Axes:       convertAxes(cell.Axes),
				Type:       "xy",
				Legend:     convertLegend(cell.Legend),
				Geom:       "line",
				ViewColors: convertColors(cell.CellColors),
				Note:       cell.Note,
				Position:   "overlaid",
			}
		case "line-stacked":
			v.Properties = influxdb.XYViewProperties{
				Queries:    convertQueries(cell.Queries),
				Axes:       convertAxes(cell.Axes),
				Type:       "xy",
				Legend:     convertLegend(cell.Legend),
				Geom:       "line", // TODO(desa): maybe this needs to be stacked?
				ViewColors: convertColors(cell.CellColors),
				Note:       cell.Note,
				Position:   "stacked",
			}
		case "line-stepplot":
			v.Properties = influxdb.XYViewProperties{
				Queries:    convertQueries(cell.Queries),
				Axes:       convertAxes(cell.Axes),
				Type:       "xy",
				Legend:     convertLegend(cell.Legend),
				Geom:       "step",
				ViewColors: convertColors(cell.CellColors),
				Note:       cell.Note,
				Position:   "overlaid",
			}
		case "bar":
			v.Properties = influxdb.XYViewProperties{
				Queries:    convertQueries(cell.Queries),
				Axes:       convertAxes(cell.Axes),
				Type:       "xy",
				Legend:     convertLegend(cell.Legend),
				Geom:       "bar",
				ViewColors: convertColors(cell.CellColors),
				Note:       cell.Note,
				Position:   "overlaid",
			}
		case "line-plus-single-stat":
			v.Properties = influxdb.LinePlusSingleStatProperties{
				Queries:    convertQueries(cell.Queries),
				Axes:       convertAxes(cell.Axes),
				Legend:     convertLegend(cell.Legend),
				ViewColors: convertColors(cell.CellColors),
				Note:       cell.Note,
				Position:   "overlaid",
			}
		case "single-stat":
			v.Properties = influxdb.EmptyViewProperties{}
		case "gauge":
		case "table":
			v.Properties = influxdb.EmptyViewProperties{}
		case "alerts":
			v.Properties = influxdb.EmptyViewProperties{}
		case "news":
			v.Properties = influxdb.EmptyViewProperties{}
		case "guide":
			v.Properties = influxdb.EmptyViewProperties{}
		case "note":
		default:
			v.Properties = influxdb.EmptyViewProperties{}
		}

		c.View = &v
		d2.Cells = append(d2.Cells, c)
	}

	return pkger.DashboardToResource(*d2, d1.Name), nil
}

func convertAxes(a map[string]chronograf.Axis) map[string]influxdb.Axis {
	m := map[string]influxdb.Axis{}
	for k, v := range a {
		m[k] = influxdb.Axis{
			Bounds: v.Bounds,
			Label:  v.Label,
			Prefix: v.Prefix,
			Suffix: v.Suffix,
			Base:   v.Base,
			Scale:  v.Scale,
		}
	}

	if _, exists := m["x"]; !exists {
		m["x"] = influxdb.Axis{}
	}
	if _, exists := m["y"]; !exists {
		m["y"] = influxdb.Axis{}
	}

	return m
}

func convertLegend(l chronograf.Legend) influxdb.Legend {
	return influxdb.Legend{
		Type:        l.Type,
		Orientation: l.Orientation,
	}
}

func convertColors(cs []chronograf.CellColor) []influxdb.ViewColor {
	vs := []influxdb.ViewColor{}

	hasTextColor := false
	for _, c := range cs {
		if c.Type == "text" {
			hasTextColor = true
		}

		v := influxdb.ViewColor{
			ID:   c.ID,
			Type: c.Type,
			Hex:  c.Hex,
			Name: c.Name,
			// TODO(desa): need to turn into hex value
			//Value: c.Value,
		}
		vs = append(vs, v)
	}

	if !hasTextColor {
		vs = append(vs, influxdb.ViewColor{
			ID:    "base",
			Type:  "text",
			Hex:   "#00C9FF",
			Name:  "laser",
			Value: 0,
		})
	}

	return vs
}

func convertQueries(qs []chronograf.DashboardQuery) []influxdb.DashboardQuery {
	ds := []influxdb.DashboardQuery{}
	for _, q := range qs {
		d := influxdb.DashboardQuery{
			// TODO(desa): possibly we should try to compile the query to flux that we can show the user.
			Text:     "// " + q.Command,
			EditMode: "advanced",
		}

		_ = q

		ds = append(ds, d)
	}

	if len(ds) == 0 {
		d := influxdb.DashboardQuery{
			// TODO(desa): possibly we should try to compile the query to flux that we can show the user.
			Text:     "// cell had no queries",
			EditMode: "advanced",
			BuilderConfig: influxdb.BuilderConfig{
				// TODO(desa): foo
				Buckets: []string{"bucket"},
			},
		}
		ds = append(ds, d)
	}

	return ds
}
