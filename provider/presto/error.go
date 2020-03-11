package presto

import "fmt"

type ErrLayerNotFound struct {
	LayerName string
}

func (e ErrLayerNotFound) Error() string {
	return fmt.Sprintf("presto: layer (%v) not found ", e.LayerName)
}

type ErrInvalidSSLMode string

func (e ErrInvalidSSLMode) Error() string {
	return fmt.Sprintf("presto: invalid ssl mode (%v)", string(e))
}

type ErrUnclosedToken string

func (e ErrUnclosedToken) Error() string {
	return fmt.Sprintf("presto: unclosed token in (%v)", string(e))
}

type ErrGeomFieldNotFound struct {
	GeomFieldName string
	LayerName     string
}

func (e ErrGeomFieldNotFound) Error() string {
	return fmt.Sprintf("presto: geom fieldname (%v) not found for layer (%v)", e.GeomFieldName, e.LayerName)
}
