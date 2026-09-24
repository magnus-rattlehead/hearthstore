package datastore

import (
	"strconv"
	"strings"
	"unicode/utf8"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// parseMaskPath uses the Java emulator's GoogleSqlPropertyPathToRepConverter
// grammar. Masks are explicit paths, not the two interpretations of query names.
func parseMaskPath(path string) ([]string, error) {
	invalid := func() ([]string, error) {
		return nil, status.Error(codes.InvalidArgument, "invalid property mask path")
	}
	var parts []string
	for path != "" {
		var name string
		if path[0] == '`' {
			path = path[1:]
			var decoded strings.Builder
			for path != "" && path[0] != '`' {
				if path[0] == '\r' {
					decoded.WriteByte('\n')
					path = strings.TrimPrefix(path[1:], "\n")
					continue
				}
				if len(path) >= 2 && path[0] == '\\' {
					switch path[1] {
					case '`', '\'', '"', '?':
						decoded.WriteByte(path[1])
						path = path[2:]
						continue
					case 'X':
						path = "\\x" + path[2:]
					}
				}
				// UnquoteChar implements the remaining C escapes (including
				// octal, hexadecimal and Unicode); Java appends their codepoint.
				char, _, tail, err := strconv.UnquoteChar(path, 0)
				if err != nil {
					return invalid()
				}
				decoded.WriteRune(char)
				path = tail
			}
			if path == "" {
				return invalid()
			}
			name, path = decoded.String(), path[1:]
		} else {
			i := 0
			for i < len(path) && (path[i] >= 'a' && path[i] <= 'z' || path[i] >= 'A' && path[i] <= 'Z' || path[i] == '_' || i > 0 && path[i] >= '0' && path[i] <= '9') {
				i++
			}
			name, path = path[:i], path[i:]
		}
		if name == "" || len(name) > maxNameBytes || !utf8.ValidString(name) {
			return invalid()
		}
		if reservedName(name) && !(len(parts) == 0 && name == "__key__" && path == "") {
			return invalid()
		}
		parts = append(parts, name)
		if path == "" {
			return parts, nil
		}
		if path[0] != '.' || len(path) == 1 {
			return invalid()
		}
		path = path[1:]
	}
	return invalid()
}

func maskedValue(entity *datastorepb.Entity, parts []string) *datastorepb.Value {
	for i, part := range parts {
		value := entity.GetProperties()[part]
		if i == len(parts)-1 {
			return value
		}
		entity = value.GetEntityValue()
	}
	return nil
}

func setMaskedValue(entity, incoming *datastorepb.Entity, parts []string, value *datastorepb.Value) {
	for i, part := range parts {
		if i == len(parts)-1 {
			if value == nil {
				delete(entity.Properties, part)
			} else {
				if entity.Properties == nil {
					entity.Properties = make(map[string]*datastorepb.Value)
				}
				entity.Properties[part] = value
			}
			return
		}
		child := entity.GetProperties()[part].GetEntityValue()
		if child == nil {
			if value == nil {
				return
			}
			child = &datastorepb.Entity{}
			if entity.Properties == nil {
				entity.Properties = make(map[string]*datastorepb.Value)
			}
			// EntityUpdater.ValueBuilder.getBuilder copies the source parent's
			// exclusion only when creating/replacing a non-entity parent.
			entity.Properties[part] = &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: child}, ExcludeFromIndexes: incoming.GetProperties()[part].GetExcludeFromIndexes()}
		}
		entity = child
		incoming = incoming.GetProperties()[part].GetEntityValue()
	}
}
