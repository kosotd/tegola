package presto

import (
	"context"
	dsql "database/sql"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-spatial/geom/slippy"
	"github.com/kosotd/tegola"
	"github.com/kosotd/tegola/basic"
	"github.com/kosotd/tegola/provider"
)

// genSQL will fill in the SQL field of a layer given a pool, and list of fields.
func genSQL(l *Layer, pool *dsql.DB, tblname string, flds []string) (sql string, err error) {

	// we need to hit the database to see what the fields are.
	if len(flds) == 0 {
		sql := fmt.Sprintf(fldsSQL, tblname)

		//	if a subquery is set in the 'sql' config the subquery is set to the layer's
		//	'tablename' param. because of this case normal SQL token replacement needs to be
		//	applied to tablename SQL generation
		tile := slippy.NewTile(0, 0, 0, 64, tegola.WebMercator)
		sql, err = replaceTokens(sql, 3857, tile)
		if err != nil {
			return "", err
		}

		rows, err := pool.Query(sql)
		if err != nil {
			return "", err
		}
		defer rows.Close()

		fdescs, err := rows.Columns()
		if err != nil {
			return "", err
		}
		if len(fdescs) == 0 {
			return "", fmt.Errorf("no fields were returned for table %v", tblname)
		}

		// to avoid field names possibly colliding with Postgres keywords,
		// we wrap the field names in quotes
		for i := range fdescs {
			flds = append(flds, fdescs[i])
		}
	}

	for i := range flds {
		flds[i] = fmt.Sprintf(`"%v"`, flds[i])
	}

	var fgeom int = -1
	var fgid bool
	for i, f := range flds {
		if f == `"`+l.geomField+`"` {
			fgeom = i
		}

		if f == `"`+l.idField+`"` {
			fgid = true
		}
	}

	// to avoid field names possibly colliding with Postgres keywords,
	// we wrap the field names in quotes
	if fgeom == -1 {
		flds = append(flds, fmt.Sprintf(`ST_AsBinary(ST_GeometryFromText("%v")) AS "%[1]v"`, l.geomField))
	} else {
		flds[fgeom] = fmt.Sprintf(`ST_AsBinary(ST_GeometryFromText("%v")) AS "%[1]v"`, l.geomField)
	}

	if !fgid && l.idField != "" {
		flds = append(flds, fmt.Sprintf(`"%v"`, l.idField))
	}

	selectClause := strings.Join(flds, ", ")

	return fmt.Sprintf(stdSQL, selectClause, tblname, l.geomField), nil
}

const (
	bboxToken             = "!BBOX!"
	zoomToken             = "!ZOOM!"
	scaleDenominatorToken = "!SCALE_DENOMINATOR!"
	pixelWidthToken       = "!PIXEL_WIDTH!"
	pixelHeightToken      = "!PIXEL_HEIGHT!"
)

// replaceTokens replaces tokens in the provided SQL string
//
// !BBOX! - the bounding box of the tile
// !ZOOM! - the tile Z value
// !SCALE_DENOMINATOR! - scale denominator, assuming 90.7 DPI (i.e. 0.28mm pixel size)
// !PIXEL_WIDTH! - the pixel width in meters, assuming 256x256 tiles
// !PIXEL_HEIGHT! - the pixel height in meters, assuming 256x256 tiles
func replaceTokens(sql string, srid uint64, tile provider.Tile) (string, error) {

	bufferedExtent, _ := tile.BufferedExtent()

	// TODO: leverage helper functions for minx / miny to make this easier to follow
	// TODO: it's currently assumed the tile will always be in WebMercator. Need to support different projections
	minGeo, err := basic.FromWebMercator(srid, basic.Point{bufferedExtent.MinX(), bufferedExtent.MinY()})
	if err != nil {
		return "", fmt.Errorf("error trying to convert tile point: %v ", err)
	}

	maxGeo, err := basic.FromWebMercator(srid, basic.Point{bufferedExtent.MaxX(), bufferedExtent.MaxY()})
	if err != nil {
		return "", fmt.Errorf("error trying to convert tile point: %v ", err)
	}

	minPt, maxPt := minGeo.AsPoint(), maxGeo.AsPoint()

	bbox := fmt.Sprintf("ST_GeometryFromText('POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))')",
		minPt.X(), minPt.Y(), maxPt.X(), minPt.Y(), maxPt.X(), maxPt.Y(), minPt.X(), maxPt.Y(), minPt.X(), minPt.Y())

	extent, _ := tile.Extent()
	// TODO: Always convert to meter if we support different projections
	pixelWidth := (extent.MaxX() - extent.MinX()) / 256
	pixelHeight := (extent.MaxY() - extent.MinY()) / 256
	scaleDenominator := pixelWidth / 0.00028 /* px size in m */

	// replace query string tokens
	z, _, _ := tile.ZXY()
	tokenReplacer := strings.NewReplacer(
		bboxToken, bbox,
		zoomToken, strconv.FormatUint(uint64(z), 10),
		scaleDenominatorToken, strconv.FormatFloat(scaleDenominator, 'f', -1, 64),
		pixelWidthToken, strconv.FormatFloat(pixelWidth, 'f', -1, 64),
		pixelHeightToken, strconv.FormatFloat(pixelHeight, 'f', -1, 64),
	)

	uppercaseTokenSQL := uppercaseTokens(sql)

	return tokenReplacer.Replace(uppercaseTokenSQL), nil
}

var tokenRe = regexp.MustCompile("![a-zA-Z0-9_-]+!")

//	uppercaseTokens converts all !tokens! to uppercase !TOKENS!. Tokens can
//	contain alphanumerics, dash and underline chars.
func uppercaseTokens(str string) string {
	return tokenRe.ReplaceAllStringFunc(str, strings.ToUpper)
}

func transformVal(valType string, val interface{}) (interface{}, error) {
	valType = regexp.MustCompile(`(?i)varchar\(\d+\)`).ReplaceAllString(valType, "varchar")
	valType = regexp.MustCompile(`(?i)decimal\(\d+,\d+\)`).ReplaceAllString(valType, "decimal")
	valType = regexp.MustCompile(`(?i)char\(\d+\)`).ReplaceAllString(valType, "char")

	switch strings.ToLower(valType) {
	default:
		switch vt := val.(type) {
		default:
			log.Printf("%v type is not supported. (Expected it to be a stringer type)", valType)
			return nil, fmt.Errorf("%v type is not supported. (Expected it to be a stringer type)", valType)
		case fmt.Stringer:
			return vt.String(), nil
		case string:
			return vt, nil
		}
	case "boolean", "char", "array(varchar)", "array", "varbinary", "varchar":
		return val, nil
	case "double", "smallint", "integer", "bigint", "decimal", "real":
		switch vt := val.(type) {
		case int8:
			return int64(vt), nil
		case int16:
			return int64(vt), nil
		case int32:
			return int64(vt), nil
		case int64, uint64:
			return vt, nil
		case uint8:
			return int64(vt), nil
		case uint16:
			return int64(vt), nil
		case uint32:
			return int64(vt), nil
		case float32:
			return float64(vt), nil
		case float64:
			return vt, nil
		default: // should never happen.
			return nil, fmt.Errorf("%v type is not supported. (should never happen)", valType)
		}
	case "timestamp", "date", "time":
		return fmt.Sprintf("%v", val), nil
	}
}

// decipherFields is responsible for processing the SQL result set, decoding geometries, ids and feature tags.
func decipherFields(ctx context.Context, geomFieldname, idFieldname string, descriptions []*dsql.ColumnType, values []interface{}) (gid uint64, geom string, tags map[string]interface{}, err error) {
	var ok bool

	tags = make(map[string]interface{})

	for i := range values {
		// do a quick check
		if err := ctx.Err(); err != nil {
			return 0, "", nil, err
		}

		// skip nil values.
		if values[i] == nil {
			continue
		}

		desc := descriptions[i]

		switch desc.Name() {
		case geomFieldname:
			if geom, ok = values[i].(string); !ok {
				return 0, "", nil, fmt.Errorf("unable to convert geometry field (%v) into bytes", geomFieldname)
			}
		case idFieldname:
			gid, err = gId(values[i])
		default:
			switch vex := values[i].(type) {
			case map[string]interface{}:
				for k, v := range vex {
					// we need to check if the key already exists. if it does, then don't overwrite it
					if _, ok := tags[k]; !ok {
						tags[k] = fmt.Sprintf("%v", v)
					}
				}
			default:
				value, err := transformVal(desc.DatabaseTypeName(), values[i])
				if err != nil {
					return gid, geom, tags, fmt.Errorf("unable to convert field [%v] (%v) of type (%v - %v) to a suitable value: %+v", i, desc.Name(), desc.ScanType(), desc.DatabaseTypeName(), values[i])
				}

				tags[desc.Name()] = value
			}
		}
	}

	return gid, geom, tags, nil
}

func gId(v interface{}) (gid uint64, err error) {
	switch aval := v.(type) {
	case float64:
		return uint64(aval), nil
	case int64:
		return uint64(aval), nil
	case uint64:
		return aval, nil
	case uint:
		return uint64(aval), nil
	case int8:
		return uint64(aval), nil
	case uint8:
		return uint64(aval), nil
	case uint16:
		return uint64(aval), nil
	case int32:
		return uint64(aval), nil
	case uint32:
		return uint64(aval), nil
	case string:
		return strconv.ParseUint(aval, 10, 64)
	default:
		return gid, fmt.Errorf("unable to convert field into a uint64")
	}
}
