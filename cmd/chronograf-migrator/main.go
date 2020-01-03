package main

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/bolt"
	"github.com/influxdata/influxdb/pkger"
	"gopkg.in/yaml.v2"
)

func main() {
	c := bolt.NewClient()
	c.Path = "/Users/michaeldesa/go/src/github.com/desa/chronograf-migrator/chronograf-v1.db"

	ctx := context.Background()

	if err := c.Open(ctx, nil, chronograf.BuildInfo{}); err != nil {
		panic(err)
	}

	dashboardStore := c.DashboardsStore

	ds, err := dashboardStore.All(ctx)
	if err != nil {
		panic(err)
	}

	pkg := &pkger.Pkg{
		APIVersion: pkger.APIVersion,
		Kind:       pkger.KindPackage,
		Metadata: pkger.Metadata{
			Description: "Dashboards from 1.x chronograf that are migrated to the new format",
			Name:        "Migrated Dashboards",
			Version:     "1",
		},
		Spec: struct {
			Resources []pkger.Resource `yaml:"resources" json:"resources"`
		}{
			Resources: make([]pkger.Resource, 0, 0),
		},
	}
	for _, d := range ds {
		r, err := Convert1To2Dashboard(&d)
		if err != nil {
			panic(err)
		}
		pkg.Spec.Resources = append(pkg.Spec.Resources, r)
		break
	}

	b, err := yaml.Marshal(pkg)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(b))
}
