# dataframe

A simple dataframe implementation to help read and write from CSV and slice up data

## Motivation

There are a bunch of dataframe libraries in Go these days, but I needed something simple that could maintain int, int64, float64, and string slices of data in a way that I could iterate through them

## Installation

Install the package with:

```
go get github.com/kcphysics/dataframe
```

Import it with:

```
import "github.com/kcphysics/dataframe"
```

## Example

Let's say you want to create a data frame that contains your favorite sports.  You could do so with:

```go
package main

import "github.com/kcphysics/dataframe"

func main() {
    favoriteSports := []string{
        "rugby",
        "hockey",
        "football",
    }
    df := dataframe.New()
    df.AddStringColumn("favorite sports", favoriteSports)
    fmt.Println(df)
}
```

While this example is pretty silly, you could add more columns if you'd like.  They will maintain order, and allow you to split them.

## License

MIT License - see LICENSE for more details
