#' Create a connection with WTM database
#'
#' Returns a connection object
#'
#' @export
wtm_connect <- function(){
    
    wtm_db <-  Sys.getenv("wtm_db")
    
    wtm_host <-  Sys.getenv("wtm_host")
    
    wtm_user <- Sys.getenv("wtm_user")
    
    wtm_password <- Sys.getenv("wtm_password")
    
    
    setup_problem <- any(c(wtm_db, wtm_host, wtm_user, wtm_password) == "")
    
    if(setup_problem){
        stop("check whether you set up your environment variables correctly!")
    }
    
    con <- DBI::dbConnect(RPostgres::Postgres(), dbname = wtm_db, host=wtm_host, user=wtm_user, password=wtm_password)
    
    return(con)
    
}

#' Get WTM impressions data
#'
#' Connect to database and retrieve (latest) WTM impressions data
#'
#' @param only_political whether only political impressions should be retrieved (defaults to `TRUE`)
#' @param from the date (as chr) from which you want impressions data
#' @param to the date (as chr) to which you want impressions data
#' @param cntry from what countr(ies) do you want political data (defaults to "DE"). Can take vectors too.
#' @param file_path If specified already present data will be updated
#' @param save_path If specified data will be
#' @param parse When `TRUE` data will be parsed
#' @return returns a tibble with requested data
#' @export
wtm_impressions <- function(only_political = T,
                            from, to = as.character(Sys.Date()), cntry = "DE",
                            file_path = NULL,
                            save_path = NULL,
                            multi = F,
                            parse = F){
    
    wtm_db <-  Sys.getenv("wtm_db")
    
    wtm_host <-  Sys.getenv("wtm_host")
    
    wtm_user <- Sys.getenv("wtm_user")
    
    wtm_password <- Sys.getenv("wtm_password")
    
    
    setup_problem <- any(c(wtm_db, wtm_host, wtm_user, wtm_password) == "")
    
    if(setup_problem){
        stop("check whether you set up your environment variables correctly!")
    }
    
    con <- DBI::dbConnect(RPostgres::Postgres(), dbname = wtm_db, host=wtm_host, user=wtm_user, password=wtm_password)
    
    # DBI::dbListTables(con)
    
    postimpressions_db <- dplyr::tbl(con, "postimpressions")
    
    if(only_political){
        
        print("only political")
        
        candidates_db <- dplyr::tbl(con, "candidates")
        
        candidates <- candidates_db %>%
            dplyr::filter(country %in% cntry) %>%
            dplyr::collect() %>%
            janitor::clean_names()
        
        candidate_ids <- candidates %>%
            dplyr::distinct(facebook_id) %>%
            dplyr::pull(facebook_id)
        
        candidates %>%
            dplyr::arrange(desc(created_at)) %>%
            dplyr::count(created_at) %>%
            print()
        
        wtm_raw <- postimpressions_db %>%
            ## only keep after date, appear in German advertisers
            dplyr::filter(postLoggedAt >= as.Date(from) & postLoggedAt <= as.Date(to) & advertiserId %in% candidate_ids) %>%
            dplyr::collect()
        
        ## Create final cleaned dataset
        wtm <<- wtm_raw %>%
            dplyr::mutate(advertiserId = as.character(advertiserId)) %>%
            dplyr::rename(advetisement_id = id) %>%
            janitor::clean_names() %>%
            dplyr::left_join(candidates %>% dplyr::mutate(advertiser_id = facebook_id), by = "advertiser_id")
        
    } else if (!only_political){
        
        print("not only political")
        
        ## Create final cleaned dataset
        wtm <- postimpressions_db %>%
            ## only keep after date
            dplyr::filter(postLoggedAt >= as.Date(from) & postLoggedAt <= as.Date(to)) %>%
            filter(wtmUserCountry == cntry) %>%
            dplyr::collect() %>%
            dplyr::mutate(advertiserId = as.character(advertiserId)) %>%
            dplyr::rename(advetisement_id = id)  %>%
            janitor::clean_names()
    }
    
    if(parse){
        
        # wtm <<- wtm
        
        if(!multi){
            wtm <- wtm %>%
                dplyr::rowwise() %>%
                dplyr::mutate(tg = list(wtm_parse_targeting(waist_targeting_data))) %>%
                dplyr::ungroup() %>%
                dplyr::mutate(temp = waist_targeting_data) %>%
                dplyr::select(-dplyr::contains("waist")) %>% 
                tidyr::unnest(tg) %>%
                dplyr::rename(waist_targeting_data = temp)     
            
        } else if (multi){
            
            plan(multicore, workers = 16)
            
            targ <- wtm %>%
                dplyr::select(waist_targeting_data, advetisement_id) %>% 
                split(1:nrow(.)) %>% 
                furrr::future_map_dfr(~{wtm_parse_targeting(.x$waist_targeting_data) %>% 
                        dplr::mutate(advetisement_id = .x$advetisement_id)}, .progress = T)
            
            wtm <- wtm %>%
                dplyr::select(-dplyr::contains("waist")) %>% 
                left_join(targ)
        }
        

    }
    
    if(!is.null(file_path)){
        old_data <- vroom::vroom(file_path, col_types = cols(.default = "c"))
        
        wtm <- wtm %>%
            mutate_all(as.character) %>% 
            dplyr::bind_rows(old_data) %>%
            dplyr::distinct(advetisement_id, .keep_all = T)
        
        message(glue::glue("New rows: {nrow(wtm) - nrow(old_data)}"))
    }
    
    
    DBI::dbDisconnect(con)
    
    if(!is.null(save_path)){
        
        vroom::vroom_write(wtm, save_path)
        
        message("File saved!")
        
    }
    
    
    message(glue::glue("Final data: {nrow(wtm)} rows."))
    
    return(wtm)
    
}
