package engine

type Qualifier map[string][]byte

// Row defines a row of data in LiteTable:
//
// Example:
//
//	Row{
//	  Key: "row1",
//	  Columns: map[string]Qualifier{
//	    "family1": {
//	      "qualifier1": []byte("value1"),
//	      "qualifier2": []byte("value2"),
//	    },
//	    "family2": {
//	      "qualifier1": []byte("value3"),
//	    },
//	  },
//	}
//	This represents a row with key "row1" containing two families: "family1" and "family2",
//	each with their respective qualifiers and values.
//
// Qualifiers are defined by your codes' logic.
type Row struct {
	Key     string
	Columns map[string]Qualifier // family -> qualifier -> value
}
