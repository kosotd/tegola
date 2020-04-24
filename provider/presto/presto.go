package presto

import (
	"context"
	dsql "database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/go-spatial/geom"
	"github.com/go-spatial/geom/encoding/wkb"
	"github.com/go-spatial/geom/slippy"
	"github.com/kosotd/tegola"
	"github.com/kosotd/tegola/provider"

	"github.com/kosotd/tegola/dict"
	_ "github.com/prestodb/presto-go-client/presto"
)

const Name = "presto"

// Provider provides the presto data provider.
type Provider struct {
	pool *dsql.DB
	// map of layer name and corresponding sql
	layers     map[string]Layer
	srid       uint64
	firstlayer string
}

const (
	// We quote the field and table names to prevent colliding with presto keywords.
	stdSQL = `SELECT %[1]v FROM %[2]v WHERE ST_Intersects(ST_GeometryFromText("%[3]v"), ` + bboxToken + ")"

	// SQL to get the column names, without hitting the information_schema. Though it might be better to hit the information_schema.
	fldsSQL = `SELECT * FROM %[1]v LIMIT 0`
)

const (
	DefaultPort    = 8080
	DefaultSRID    = tegola.WebMercator
	DefaultMaxConn = 100
)

const (
	ConfigKeyHost        = "host"
	ConfigKeyPort        = "port"
	ConfigKeyCatalog     = "catalog"
	ConfigKeyScheme      = "scheme"
	ConfigKeyUser        = "user"
	ConfigKeyPassword    = "password"
	ConfigKeyMaxConn     = "max_connections"
	ConfigKeySRID        = "srid"
	ConfigKeyLayers      = "layers"
	ConfigKeyLayerName   = "name"
	ConfigKeyTablename   = "tablename"
	ConfigKeySQL         = "sql"
	ConfigKeyFields      = "fields"
	ConfigKeyGeomField   = "geometry_fieldname"
	ConfigKeyGeomIDField = "id_fieldname"
	ConfigKeyGeomType    = "geometry_type"
)

func init() {
	_ = provider.Register(Name, NewTileProvider, Cleanup)
}

// isSelectQuery is a regexp to check if a query starts with `SELECT`,
// case-insensitive and ignoring any preceeding whitespace and SQL comments.
var isSelectQuery = regexp.MustCompile(`(?i)^((\s*)(--.*\n)?)*select`)

// NewTileProvider instantiates and returns a new presto provider or an error.
// The function will validate that the config object looks good before
// trying to create a driver. This Provider supports the following fields
// in the provided map[string]interface{} map:
//
// 	host (string): [Required] presto database host
// 	port (int): [Required] presto database port
// 	catalog (string): [Required] presto database catalog
// 	scheme (string): [Required] presto database scheme
// 	user (string): [Required] presto database user
// 	password (string): [Required] presto database password
// 	srid (int): [Optional] The default SRID for the provider. Defaults to WebMercator (3857) but also supports WGS84 (4326)
// 	max_connections : [Optional] The max connections to maintain in the connection pool. Default is 100. 0 means no max.
// 	layers (map[string]struct{})  — This is map of layers keyed by the layer name. supports the following properties
//
// 		name (string): [Required] the name of the layer. This is used to reference this layer from map layers.
// 		tablename (string): [*Required] the name of the database table to query against. Required if sql is not defined.
// 		geometry_fieldname (string): [Optional] the name of the filed which contains the geometry for the feature. defaults to geom
// 		id_fieldname (string): [Optional] the name of the feature id field. defaults to gid
// 		fields ([]string): [Optional] a list of fields to include alongside the feature. Can be used if sql is not defined.
// 		srid (int): [Optional] the SRID of the layer. Supports 3857 (WebMercator) or 4326 (WGS84).
// 		sql (string): [*Required] custom SQL to use use. Required if tablename is not defined. Supports the following tokens:
//
// 			!BBOX! - [Required] will be replaced with the bounding box of the tile before the query is sent to the database.
// 			!ZOOM! - [Optional] will be replaced with the "Z" (zoom) value of the requested tile.
//
func NewTileProvider(config dict.Dicter) (provider.Tiler, error) {

	host, err := config.String(ConfigKeyHost, nil)
	if err != nil {
		return nil, err
	}

	catalog, err := config.String(ConfigKeyCatalog, nil)
	if err != nil {
		return nil, err
	}

	scheme, err := config.String(ConfigKeyScheme, nil)
	if err != nil {
		return nil, err
	}

	user, err := config.String(ConfigKeyUser, nil)
	if err != nil {
		return nil, err
	}

	password, err := config.String(ConfigKeyPassword, nil)
	if err != nil {
		return nil, err
	}

	port := DefaultPort
	if port, err = config.Int(ConfigKeyPort, &port); err != nil {
		return nil, err
	}

	maxcon := DefaultMaxConn
	if maxcon, err = config.Int(ConfigKeyMaxConn, &maxcon); err != nil {
		return nil, err
	}

	srid := DefaultSRID
	if srid, err = config.Int(ConfigKeySRID, &srid); err != nil {
		return nil, err
	}

	_ = fmt.Sprintf("%s %s", user, password)
	url := fmt.Sprintf("http://user@%s:%d?catalog=%s&schema=%s", host, port, catalog, scheme)

	p := Provider{
		srid: uint64(srid),
	}

	if p.pool, err = dsql.Open("presto", url); err != nil {
		return nil, fmt.Errorf("failed while creating presto connection: %v", err)
	}

	layers, err := config.MapSlice(ConfigKeyLayers)
	if err != nil {
		return nil, err
	}

	lyrs := make(map[string]Layer)
	lyrsSeen := make(map[string]int)

	for i, layer := range layers {

		lname, err := layer.String(ConfigKeyLayerName, nil)
		if err != nil {
			return nil, fmt.Errorf("for layer (%v) we got the following error trying to get the layer's name field: %v", i, err)
		}

		if j, ok := lyrsSeen[lname]; ok {
			return nil, fmt.Errorf("%v layer name is duplicated in both layer %v and layer %v", lname, i, j)
		}

		lyrsSeen[lname] = i
		if i == 0 {
			p.firstlayer = lname
		}

		fields, err := layer.StringSlice(ConfigKeyFields)
		if err != nil {
			return nil, fmt.Errorf("for layer (%v) %v %v field had the following error: %v", i, lname, ConfigKeyFields, err)
		}

		geomfld := "geom"
		geomfld, err = layer.String(ConfigKeyGeomField, &geomfld)
		if err != nil {
			return nil, fmt.Errorf("for layer (%v) %v : %v", i, lname, err)
		}

		idfld := ""
		idfld, err = layer.String(ConfigKeyGeomIDField, &idfld)
		if err != nil {
			return nil, fmt.Errorf("for layer (%v) %v : %v", i, lname, err)
		}
		if idfld == geomfld {
			return nil, fmt.Errorf("for layer (%v) %v: %v (%v) and %v field (%v) is the same", i, lname, ConfigKeyGeomField, geomfld, ConfigKeyGeomIDField, idfld)
		}

		geomType := ""
		geomType, err = layer.String(ConfigKeyGeomType, &geomType)
		if err != nil {
			return nil, fmt.Errorf("for layer (%v) %v : %v", i, lname, err)
		}

		var tblName string
		tblName, err = layer.String(ConfigKeyTablename, &lname)
		if err != nil {
			return nil, fmt.Errorf("for %v layer (%v) %v has an error: %v", i, lname, ConfigKeyTablename, err)
		}

		var sql string
		sql, err = layer.String(ConfigKeySQL, &sql)
		if err != nil {
			return nil, fmt.Errorf("for %v layer (%v) %v has an error: %v", i, lname, ConfigKeySQL, err)
		}

		if tblName != lname && sql != "" {
			log.Printf("both %v and %v field are specified for layer (%v) %v, using only %[2]v field.", ConfigKeyTablename, ConfigKeySQL, i, lname)
		}

		var lsrid = srid
		if lsrid, err = layer.Int(ConfigKeySRID, &lsrid); err != nil {
			return nil, err
		}

		l := Layer{
			name:      lname,
			idField:   idfld,
			geomField: geomfld,
			srid:      uint64(lsrid),
		}

		if sql != "" && !isSelectQuery.MatchString(sql) {
			// if it is not a SELECT query, then we assume we have a sub-query
			// (`(select ...) as foo`) which we can handle like a tablename
			tblName = sql
			sql = ""
		}

		if sql != "" {
			// convert !BOX! (MapServer) and !bbox! (Mapnik) to !BBOX! for compatibility
			sql := strings.Replace(strings.Replace(sql, "!BOX!", "!BBOX!", -1), "!bbox!", "!BBOX!", -1)
			// make sure that the sql has a !BBOX! token
			if !strings.Contains(sql, bboxToken) {
				return nil, fmt.Errorf("SQL for layer (%v) %v is missing required token: %v", i, lname, bboxToken)
			}
			if !strings.Contains(sql, "*") {
				if !strings.Contains(sql, geomfld) {
					return nil, fmt.Errorf("SQL for layer (%v) %v does not contain the geometry field: %v", i, lname, geomfld)
				}
				if !strings.Contains(sql, idfld) {
					return nil, fmt.Errorf("SQL for layer (%v) %v does not contain the id field for the geometry: %v", i, lname, idfld)
				}
			}

			l.sql = sql
		} else {
			// Tablename and Fields will be used to build the query.
			// We need to do some work. We need to check to see Fields contains the geom and gid fields
			// and if not add them to the list. If Fields list is empty/nil we will use '*' for the field list.
			l.sql, err = genSQL(&l, p.pool, tblName, fields)
			if err != nil {
				return nil, fmt.Errorf("could not generate sql, for layer(%v): %v", lname, err)
			}
		}

		if strings.Contains(os.Getenv("TEGOLA_SQL_DEBUG"), "LAYER_SQL") {
			log.Printf("SQL for Layer(%v):\n%v\n", lname, l.sql)
		}

		// set the layer geom type
		if geomType != "" {
			if err = p.setLayerGeomType(&l, geomType); err != nil {
				return nil, fmt.Errorf("error fetching geometry type for layer (%v): %v", l.name, err)
			}
		} else {
			if err = p.inspectLayerGeomType(&l); err != nil {
				return nil, fmt.Errorf("error fetching geometry type for layer (%v): %v", l.name, err)
			}
		}

		lyrs[lname] = l
	}
	p.layers = lyrs

	// track the provider so we can clean it up later
	providers = append(providers, p)

	return p, nil
}

// setLayerGeomType sets the geomType field on the layer to one of point,
// linestring, polygon, multipoint, multilinestring, multipolygon or
// geometrycollection
func (p Provider) setLayerGeomType(l *Layer, geomType string) error {
	switch strings.ToLower(geomType) {
	case "point":
		l.geomType = geom.Point{}
	case "linestring":
		l.geomType = geom.LineString{}
	case "polygon":
		l.geomType = geom.Polygon{}
	case "multipoint":
		l.geomType = geom.MultiPoint{}
	case "multilinestring":
		l.geomType = geom.MultiLineString{}
	case "multipolygon":
		l.geomType = geom.MultiPolygon{}
	case "geometrycollection":
		l.geomType = geom.Collection{}
	default:
		return fmt.Errorf("unsupported geometry_type (%v) for layer (%v)", geomType, l.name)
	}
	return nil
}

// inspectLayerGeomType sets the geomType field on the layer by running the SQL
// and reading the geom type in the result set
func (p Provider) inspectLayerGeomType(l *Layer) error {
	var err error

	// we want to know the geom type instead of returning the geom data so we modify the SQL
	// TODO (arolek): this strategy wont work if remove the requirement of wrapping ST_AsBinary(geom) in the SQL statements.
	//
	// https://github.com/kosotd/tegola/issues/180
	//
	// case insensitive search
	re := regexp.MustCompile(`(?i)ST_AsBinary`)
	sql := re.ReplaceAllString(l.sql, "ST_GeometryType")

	// we only need a single result set to sniff out the geometry type
	sql = fmt.Sprintf("%v LIMIT 1", sql)

	// if a !ZOOM! token exists, all features could be filtered out so we don't have a geometry to inspect it's type.
	// address this by replacing the !ZOOM! token with an ANY statement which includes all zooms
	sql = strings.Replace(sql, "!ZOOM!", "ANY('{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24}')", 1)

	// we need a tile to run our sql through the replacer
	tile := slippy.NewTile(0, 0, 0, 64, tegola.WebMercator)

	// normal replacer
	sql, err = replaceTokens(sql, l.srid, tile)
	if err != nil {
		return err
	}

	rows, err := p.pool.Query(sql)
	if err != nil {
		return err
	}
	defer rows.Close()

	// fetch rows FieldDescriptions. this gives us the OID for the data types returned to aid in decoding
	fdescs, err := rows.Columns()
	if err != nil {
		return err
	}

	vals := make([]interface{}, len(fdescs))
	ptrs := make([]interface{}, len(fdescs))
	for i := 0; i < len(vals); i++ {
		ptrs[i] = &vals[i]
	}

	for rows.Next() {

		err := rows.Scan(ptrs...)
		if err != nil {
			return fmt.Errorf("error running SQL: %v ; %v", sql, err)
		}

		// iterate the values returned from our row, sniffing for the geomField or st_geometrytype field name
		for i, v := range vals {
			switch fdescs[i] {
			case l.geomField, "st_geometrytype":
				switch v {
				case "ST_Point":
					l.geomType = geom.Point{}
				case "ST_LineString":
					l.geomType = geom.LineString{}
				case "ST_Polygon":
					l.geomType = geom.Polygon{}
				case "ST_MultiPoint":
					l.geomType = geom.MultiPoint{}
				case "ST_MultiLineString":
					l.geomType = geom.MultiLineString{}
				case "ST_MultiPolygon":
					l.geomType = geom.MultiPolygon{}
				case "ST_GeometryCollection":
					l.geomType = geom.Collection{}
				default:
					return fmt.Errorf("layer (%v) returned unsupported geometry type (%v)", l.name, v)
				}
			}
		}
	}

	return rows.Err()
}

// Layer fetches an individual layer from the provider, if it's configured
// if no name is provider, the first layer is returned
func (p *Provider) Layer(name string) (Layer, bool) {
	if name == "" {
		return p.layers[p.firstlayer], true
	}

	layer, ok := p.layers[name]
	return layer, ok
}

// Layers returns meta data about the various layers which are configured with the provider
func (p Provider) Layers() ([]provider.LayerInfo, error) {
	var ls []provider.LayerInfo

	for i := range p.layers {
		ls = append(ls, p.layers[i])
	}

	return ls, nil
}

// TileFeatures adheres to the provider.Tiler interface
func (p Provider) TileFeatures(ctx context.Context, layer string, tile provider.Tile, fn func(f *provider.Feature) error) error {
	// fetch the provider layer
	plyr, ok := p.Layer(layer)
	if !ok {
		return ErrLayerNotFound{layer}
	}

	sql, err := replaceTokens(plyr.sql, plyr.srid, tile)
	if err != nil {
		return fmt.Errorf("error replacing layer tokens for layer (%v) SQL (%v): %v", layer, sql, err)
	}

	if strings.Contains(os.Getenv("TEGOLA_SQL_DEBUG"), "EXECUTE_SQL") {
		log.Printf("TEGOLA_SQL_DEBUG:EXECUTE_SQL for layer (%v): %v", layer, sql)
	}

	// context check
	if err := ctx.Err(); err != nil {
		return err
	}

	rows, err := p.pool.Query(sql)
	if err != nil {
		return fmt.Errorf("error running layer (%v) SQL (%v): %v", layer, sql, err)
	}
	defer rows.Close()

	// fetch rows FieldDescriptions. this gives us the OID for the data types returned to aid in decoding
	fdescs, err := rows.Columns()
	if err != nil {
		return err
	}

	// loop our field descriptions looking for the geometry field
	var geomFieldFound bool
	for i := range fdescs {
		if fdescs[i] == plyr.GeomFieldName() {
			geomFieldFound = true
			break
		}
	}
	if !geomFieldFound {
		return ErrGeomFieldNotFound{
			GeomFieldName: plyr.GeomFieldName(),
			LayerName:     plyr.Name(),
		}
	}

	vals := make([]interface{}, len(fdescs))
	ptrs := make([]interface{}, len(fdescs))
	for i := 0; i < len(vals); i++ {
		ptrs[i] = &vals[i]
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for rows.Next() {
		// context check
		if err := ctx.Err(); err != nil {
			return err
		}

		// fetch row values
		err := rows.Scan(ptrs...)
		if err != nil {
			return fmt.Errorf("error running layer (%v) SQL (%v): %v", layer, sql, err)
		}

		gid, geobytes, tags, err := decipherFields(ctx, plyr.GeomFieldName(), plyr.IDFieldName(), types, vals)
		if err != nil {
			switch err {
			case context.Canceled:
				return err
			default:
				return fmt.Errorf("for layer (%v) %v", plyr.Name(), err)
			}
		}

		// check that we have geometry data. if not, skip the feature
		if len(geobytes) == 0 {
			continue
		}

		wkbGeom, err := base64.StdEncoding.DecodeString(geobytes)
		if err != nil {
			return fmt.Errorf("error decoding base64 geometry in layer: (%v) SQL (%v): %v", layer, sql, err)
		}
		// decode our WKB
		geometry, err := wkb.DecodeBytes(wkbGeom)
		if err != nil {
			switch err.(type) {
			case wkb.ErrUnknownGeometryType:
				continue
			default:
				return fmt.Errorf("unable to decode layer (%v) geometry field (%v) into wkb where (%v = %v): %v", layer, plyr.GeomFieldName(), plyr.IDFieldName(), gid, err)
			}
		}

		feature := provider.Feature{
			ID:       gid,
			Geometry: geometry,
			SRID:     plyr.SRID(),
			Tags:     tags,
		}

		// pass the feature to the provided callback
		if err = fn(&feature); err != nil {
			return err
		}
	}

	return rows.Err()
}

// Close will close the Provider's database connectio
func (p *Provider) Close() { _ = p.pool.Close() }

// reference to all instantiated providers
var providers []Provider

// Cleanup will close all database connections and destroy all previously instantiated Provider instances
func Cleanup() {
	if len(providers) > 0 {
		log.Printf("cleaning up presto providers")
	}

	for i := range providers {
		providers[i].Close()
	}

	providers = make([]Provider, 0)
}