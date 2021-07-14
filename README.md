
<!-- README.md is generated from README.Rmd. Please edit that file -->

# wtmr

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://www.tidyverse.org/lifecycle/#experimental)
<!-- badges: end -->

`wtmr` is a helper package to access, update, parse and analyze Who
Targets Me data.

## Installation

You can install the development version of `wtmr` from GitHub with:

``` r
remotes::install_github("WhoTargetsMe/wtmr")
```

## Examples

``` r
library(wtmr)
```

Establishing a connection:

``` r
wtm_con <- wtm_connect()
```

Retrieving data:

``` r
wtm_imps <- wtm_impressions(from = "2021-07-10")
```
