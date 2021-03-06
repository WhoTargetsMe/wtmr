% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/data.R
\name{wtm_impressions}
\alias{wtm_impressions}
\title{Get WTM impressions data}
\usage{
wtm_impressions(
  only_political = T,
  from,
  cntry = "DE",
  file_path = NULL,
  save_path = NULL,
  parse = F
)
}
\arguments{
\item{only_political}{whether only political impressions should be retrieved (defaults to \code{TRUE})}

\item{from}{the date (as chr) from which you want impressions data}

\item{cntry}{from what countr(ies) do you want political data (defaults to "DE"). Can take vectors too.}

\item{file_path}{If specified already present data will be updated}

\item{save_path}{If specified data will be}

\item{parse}{When \code{TRUE} data will be parsed}
}
\value{
returns a tibble with requested data
}
\description{
Connect to database and retrieve (latest) WTM impressions data
}
