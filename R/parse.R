#' Parse WTM targeting data
#'
#' @param col column that includes raw JSON
#' @param p used for iterator for progressr
#' @return returns a tibble
#' @export
wtm_parse_targeting <- function(col, p) {

  if(!missing(p)){
    p()
  }

  cols_to_add <- c("waist_custom_audience", "waist_ca_engagement_page",
                   "waist_ca_engagement_ig", "waist_ca_pixel", "waist_interest",
                   "waist_language", "waist_education_status", "waist_friends_connection",
                   "waist_connection", "waist_ca_lookalike", "waist_ca_engagement_video",
                   "waist_bct", "waist_employers", "waist_ca_datafile",
                   "waist_ca_engagement_event", "waist_school", "waist_job_titles",
                   "waist_dynamic_rule")

  if(is.na(col)){
    return(tibble::tibble(row_na = T) %>% add_cols(cols_to_add))
  }

  df_raw <- col %>%
    jsonlite::fromJSON() %>%
    purrr::map_dfc(wtm_parse_json) %>%
    add_cols(cols_to_add) %>%
    dplyr::mutate(waist_custom_audience = ifelse(waist_ca_pixel | waist_ca_engagement_page |
                                                   waist_ca_engagement_ig | waist_ca_engagement_video |
                                                   waist_ca_datafile | waist_ca_lookalike |
                                                   waist_ca_engagement_event, T, F))

  if(df_raw$waist_custom_audience){
    df_raw <- df_raw %>%
      tidyr::unite("waist_ca_type", dplyr::contains("waist_ca_type"), remove = T, na.rm = T, sep = ", ") %>%
      tidyr::unite("waist_ca_type_id", dplyr::contains("ca_type_"), remove = T, na.rm = T, sep = ", ") %>%
      tidyr::unite("waist_ca_fbid", dplyr::contains("fbid"), remove = T, na.rm = T, sep = ", ") %>%
      dplyr::mutate(waist_ca_type_id = ifelse(waist_ca_type_id == "", NA, waist_ca_type_id),
                    waist_ca_type = ifelse(waist_ca_type == "", NA, waist_ca_type),
                    waist_ca_fbid = ifelse(waist_ca_fbid == "", NA, waist_ca_fbid))

    if(any(stringr::str_detect(names(df_raw),"match_keys"))){
      df_raw <- df_raw %>%
        tidyr::unite("waist_ca_match_keys", dplyr::contains("match_keys"), remove = T, na.rm = T, sep = ", ")%>%
        dplyr::mutate(waist_ca_match_keys = ifelse(waist_ca_match_keys == "", NA, waist_ca_match_keys))
    }

  }

  df_cols <- df_raw %>%
    dplyr::select(dplyr::contains("waist"), dplyr::everything())  %>%
    dplyr::mutate(waist_employment = ifelse(waist_job_titles | waist_employers, T, F),
                  waist_edu = ifelse(waist_education_status | waist_school, T, F))

  return(df_cols)
}

#' Parse WTM targeting JSON data
#'
#' @param raw raw JSON
#' @return returns a tibble
#' @export
wtm_parse_json <- function(raw) {
    
    # print(raw)
    # raw <<- raw
    
    known_waist <- c("INTERESTS",
                     "ED_STATUS",
                     "EDU_SCHOOLS",
                     "CUSTOM_AUDIENCES_WEBSITE",
                     "CUSTOM_AUDIENCES_ENGAGEMENT_PAGE",
                     "CUSTOM_AUDIENCES_ENGAGEMENT_IG",
                     "CUSTOM_AUDIENCES_ENGAGEMENT_EVENT",
                     "CUSTOM_AUDIENCES_ENGAGEMENT_VIDEO",
                     "CUSTOM_AUDIENCES_LOOKALIKE",
                     "CUSTOM_AUDIENCES_DATAFILE",
                     "CUSTOM_AUDIENCES_ENGAGEMENT_LEAD_GEN",
                     "CUSTOM_AUDIENCES_ENGAGEMENT_CANVAS",
                     "CUSTOM_AUDIENCES_MOBILE_APP",
                     "ACTIONABLE_INSIGHTS",
                     "COLLABORATIVE_AD",
                     "CONNECTION",
                     "FRIENDS_OF_CONNECTION",
                     "AGE_GENDER",
                     "LOCATION",
                     "LOCALE",
                     "BCT",
                     "WORK_EMPLOYERS",
                     "WORK_JOB_TITLES",
                     "RELATIONSHIP_STATUS",
                     "DYNAMIC_RULE",
                     "LOCAL_REACH")
    
    if(!(raw$waist_ui_type %in% known_waist)){
        
        print(raw$waist_ui_type)
        
        
        warning("unknown waist type")
        
    } else if(raw$waist_ui_type == "INTERESTS"){
        # interests_dat <<- raw
        raw_out <- raw %>%
            purrr::pluck("interests") %>%
            dplyr::summarise(waist_interests = paste0(name, collapse = " ||| "),
                             waist_interests_id = paste0(id, collapse = " ||| ")) %>%
            dplyr::mutate(waist_interest = T)
    } else if(raw$waist_ui_type == "LOCALE"){
        raw_out <- raw %>%
            purrr::pluck("serialized_data") %>%
            jsonlite::fromJSON() %>%
            dplyr::bind_cols() %>%
            dplyr::mutate(languages = raw$locales) %>%
            dplyr::summarise(waist_languages = paste0(languages, collapse = " ||| "),
                             waist_languages_id = paste0(locales, collapse = " ||| ")) %>%
            dplyr::mutate(waist_language = T)
    } else if(raw$waist_ui_type == "ED_STATUS"){
        raw_out <- raw  %>%
            purrr::pluck("serialized_data") %>%
            jsonlite::fromJSON() %>%
            dplyr::bind_cols() %>%
            dplyr::mutate(education_status = raw$edu_status) %>%
            dplyr::summarise(waist_edu_status = paste0(education_status, collapse = " ||| "),
                             waist_edu_status_id = paste0(edu_status, collapse = " ||| ")) %>%
            dplyr::mutate(waist_education_status = T)
    } else if(raw$waist_ui_type == "EDU_SCHOOLS"){
        raw_out <- raw  %>%
            purrr::pluck("serialized_data") %>%
            jsonlite::fromJSON() %>%
            dplyr::bind_cols(dplyr::bind_cols(raw)) %>%
            dplyr::summarise(waist_schools = paste0(school_names, collapse = " ||| "),
                             waist_schools_id = paste0(school_ids, collapse = " ||| ")) %>%
            dplyr::mutate(waist_school = T)
    } else if(raw$waist_ui_type == "AGE_GENDER"){
        raw_out <- raw  %>%
            dplyr::bind_cols() %>%
            dplyr::rename(waist_age_min = age_min,
                          waist_age_max = age_max,
                          waist_gender = gender)
    } else if(raw$waist_ui_type == "CONNECTION"){
        raw_out <- raw %>%
            dplyr::bind_cols() %>%
            dplyr::rename(waist_connection_name = name,
                          waist_connection_type = type,
                          waist_connection_fb_id = connection_fbid) %>%
            dplyr::mutate(waist_connection = T)
    } else if(raw$waist_ui_type == "FRIENDS_OF_CONNECTION"){
        raw_out <- raw %>%
            dplyr::bind_cols() %>%
            dplyr::rename(waist_friends_connection_name = name,
                          waist_friends_connection_type = type,
                          waist_friends_connection_fb_id = connection_fbid) %>%
            dplyr::mutate(waist_friends_connection = T)
    } else if(raw$waist_ui_type == "LOCATION"){
        raw_out <- raw    %>%
            rlist::list.flatten() %>%
            purrr::pluck("serialized_data") %>%
            jsonlite::fromJSON()  %>%
            dplyr::bind_cols(dplyr::bind_cols(rlist::list.flatten(raw))) %>%
            janitor::clean_names() %>%
            dplyr::mutate(waist_location = T) %>%
            dplyr::rename(waist_location_name = location_name,
                          waist_location_type = location_type,
                          waist_location_code = location_code,
                          waist_location_granularity = location_granularity)
    } else if(raw$waist_ui_type == "BCT"){
        raw_out <- raw %>%
            dplyr::bind_cols() %>%
            dplyr::mutate(waist_bct = T) %>%
            dplyr::rename(waist_bct_name = name,
                          waist_bct_desc = desc,
                          waist_bct_id = bct_id)
    } else if(raw$waist_ui_type == "WORK_EMPLOYERS"){
        raw_out <- raw    %>%
            purrr::pluck("serialized_data") %>%
            jsonlite::fromJSON()  %>%
            dplyr::bind_cols(dplyr::bind_cols(raw)) %>%
            # janitor::clean_names() %>%
            dplyr::mutate(waist_employers = T) %>%
            dplyr::rename(waist_employer_name = employer_name,
                          waist_employer_id = work_employer)
    } else if(raw$waist_ui_type == "WORK_JOB_TITLES"){
        raw_out <- raw    %>%
            purrr::pluck("serialized_data") %>%
            jsonlite::fromJSON()  %>%
            dplyr::bind_cols(dplyr::bind_cols(raw) %>% dplyr::rename(waist_job_title = job_title)) %>%
            # janitor::clean_names() %>%
            dplyr::mutate(waist_job_titles = T) %>%
            dplyr::rename(waist_job_title_id = job_title)
    }  else if(raw$waist_ui_type == "RELATIONSHIP_STATUS"){
        raw_out <- raw    %>%
            purrr::pluck("serialized_data") %>%
            jsonlite::fromJSON()  %>%
            dplyr::bind_cols(dplyr::bind_cols(raw) %>% dplyr::rename(waist_relationship_status = relationship_status)) %>%
            # janitor::clean_names() %>%
            dplyr::mutate(waist_relationship = T) %>%
            dplyr::rename(waist_relationship_status_id = relationship_status)
    }  else if(raw$waist_ui_type == "DYNAMIC_RULE"){
        raw_out <- raw    %>%
            purrr::pluck("serialized_data") %>%
            jsonlite::fromJSON()  %>%
            dplyr::bind_cols(dplyr::bind_cols(raw)) %>%
            # janitor::clean_names() %>%
            dplyr::mutate(waist_dynamic_rule = T) %>%
            dplyr::rename(waist_dynamic_rule_type = audience_type,
                          waist_dynamic_rule_id = audience_id)
    } else if(stringr::str_detect(raw$waist_ui_type, "CUSTOM_AUDIENCES")){
        raw_out <- raw  %>%
            rlist::list.flatten() %>%
            purrr::pluck("serialized_data") %>%
            jsonlite::fromJSON() %>%
            dplyr::bind_cols(dplyr::bind_cols(rlist::list.flatten(raw))) %>%
            janitor::clean_names() %>%
            dplyr::mutate(waist_ca_type = raw$waist_ui_type) %>%
            dplyr::rename_at(dplyr::vars(dplyr::contains(c("waist_ca", "business_id", "ca_fbid",
                                                           "ca_type", "current_time_string", "is_opted_out",
                                                           "is_unhandled_dfca", "upload_time_string"))),
                             ~paste0(.x, "_", raw$waist_ui_type))
        
        
        if(raw$waist_ui_type == "CUSTOM_AUDIENCES_WEBSITE"){
            raw_out <- raw_out %>%
                dplyr::mutate(waist_ca_pixel = T) %>%
                add_cols("website_ca_data_website_url", default = NA_character_) %>%
                dplyr::rename(waist_ca_url = website_ca_data_website_url)
        } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_ENGAGEMENT_PAGE"){
            raw_out <- raw_out %>%
                dplyr::mutate(waist_ca_engagement_page = T)
        } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_ENGAGEMENT_IG"){
            raw_out <- raw_out %>%
                dplyr::mutate(waist_ca_engagement_ig = T)
        } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_LOOKALIKE"){
            raw_out <- raw_out %>%
                dplyr::mutate(waist_ca_lookalike = T)
        } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_ENGAGEMENT_VIDEO"){
            raw_out <- raw_out %>%
                dplyr::mutate(waist_ca_engagement_video = T)
        } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_DATAFILE"){
            raw_out <- raw_out %>%
                dplyr::mutate(waist_ca_datafile = T) %>%
                add_cols("dfca_data_match_keys", default = NA_character_) %>%
                dplyr::rename(waist_match_keys = dfca_data_match_keys)
        } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_ENGAGEMENT_EVENT"){
            raw_out <- raw_out %>%
                dplyr::mutate(waist_ca_engagement_event = T)
        } else if(str_detect(raw$waist_ui_type, "LEAD_GEN", negate = F)){
            raw_out <- raw_out %>%
                dplyr::mutate(waist_ca_lead_gen = T) 
        }  else if(str_detect(raw$waist_ui_type, "_CANVAS", negate = F)){
            raw_out <- raw_out %>%
                dplyr::mutate(waist_ca_canvas = T) 
        }   else if(str_detect(raw$waist_ui_type, "CUSTOM_AUDIENCES_MOBILE_APP", negate = F)){
            raw_out <- raw_out %>%
                dplyr::mutate(waist_ca_mobile_app = T) 
        } 
        
        
        
    }  else if(raw$waist_ui_type == "LOCAL_REACH"){
        
        raw_out <- tibble(waist_ui_type = "LOCAL_REACH") %>%
            dplyr::mutate(waist_local_reach = T)
    }   else if(raw$waist_ui_type == "ACTIONABLE_INSIGHTS"){
        
        raw_out <- raw  %>%
            purrr::pluck("serialized_data") %>%
            jsonlite::fromJSON()  %>%
            as_tibble() %>% 
            # janitor::clean_names() %>%
            dplyr::mutate(waist_actionable_insights = T) %>%
            dplyr::rename(waist_actionable_insights_name = name,
                          waist_actionable_insights_description = description,
                          waist_actionable_insights_id = id)
        
    } else if(raw$waist_ui_type == "COLLABORATIVE_AD"){
        
        raw_out <- raw  %>%
            purrr::pluck("serialized_data") %>%
            jsonlite::fromJSON()  %>%
            as_tibble() %>% 
            # janitor::clean_names() %>%
            dplyr::mutate(waist_collaborative_ad = T) %>%
            dplyr::rename(waist_collaborative_ad_name = merchant_name)
        
    } 
    
    if(!exists("raw_out")){
        raw_out <- tibble(waist_ui_type = raw$waist_ui_type) %>%
            dplyr::mutate(waist_other = T)     
    }
    
    
    
    raw_out <- raw_out %>%
        janitor::clean_names() %>%
        dplyr::select(-dplyr::contains("serialized_data"),
                      -dplyr::contains("typename"),
                      -dplyr::contains("ui_type"),
                      -dplyr::contains("ca_owner"),
                      -dplyr::contains("is_unhandled"),
                      -dplyr::contains("is_opted_out"),
                      -dplyr::contains("current_time"),
                      -dplyr::contains("upload_time"),
                      -dplyr::contains("birthday"),
                      -dplyr::contains("location_geo_type"),
                      -dplyr::contains("event_time"),
                      -dplyr::contains("business_id"),
                      -dplyr::matches("\\bid\\b"))
    
    return(raw_out)
}



#' Parse WTM targeting JSON data into list columns
#'
#' @param raw raw JSON
#' @return returns a tibble
#' @export
wtm_parse_json2 <- function(raw) {

  # print(raw$waist_ui_type)

  known_waist <- c("INTERESTS",
                   "ED_STATUS",
                   "EDU_SCHOOLS",
                   "CUSTOM_AUDIENCES_WEBSITE",
                   "CUSTOM_AUDIENCES_ENGAGEMENT_PAGE",
                   "CUSTOM_AUDIENCES_ENGAGEMENT_IG",
                   "CUSTOM_AUDIENCES_ENGAGEMENT_EVENT",
                   "CUSTOM_AUDIENCES_ENGAGEMENT_VIDEO",
                   "CUSTOM_AUDIENCES_LOOKALIKE",
                   "CUSTOM_AUDIENCES_DATAFILE",
                   "CONNECTION",
                   "FRIENDS_OF_CONNECTION",
                   "AGE_GENDER",
                   "LOCATION",
                   "LOCALE",
                   "BCT",
                   "WORK_EMPLOYERS",
                   "WORK_JOB_TITLES",
                   "RELATIONSHIP_STATUS",
                   "DYNAMIC_RULE")

  if(!(raw$waist_ui_type %in% known_waist)){

    print(raw$waist_ui_type)

    stop("unknown waist type")

  } else if(!(raw$waist_ui_type %in% c("INTERESTS", "LOCALE"))){

    return(invisible())

  } else if(raw$waist_ui_type == "INTERESTS"){
    # interests_dat <<- raw
    raw_out <- raw %>%
      purrr::pluck("interests") %>%
      dplyr::rename(waist_interests = name,
                    waist_interests_id = id) %>%
      dplyr::mutate(waist_interest = T)
  } else if(raw$waist_ui_type == "LOCALE"){
    raw_out <- raw %>%
      purrr::pluck("serialized_data") %>%
      jsonlite::fromJSON() %>%
      dplyr::bind_cols() %>%
      dplyr::mutate(languages = raw$locales) %>%
      dplyr::rename(waist_languages = languages,
                    waist_languages_id = locales) %>%
      dplyr::mutate(waist_language = T)
  }




  raw_out <- raw_out %>%
    janitor::clean_names() %>%
    dplyr::select(-dplyr::contains("serialized_data"),
                  -dplyr::contains("typename"),
                  -dplyr::contains("ui_type"),
                  -dplyr::contains("ca_owner"),
                  -dplyr::contains("is_unhandled"),
                  -dplyr::contains("is_opted_out"),
                  -dplyr::contains("current_time"),
                  -dplyr::contains("upload_time"),
                  -dplyr::contains("birthday"),
                  -dplyr::contains("location_geo_type"),
                  -dplyr::contains("event_time"),
                  -dplyr::contains("business_id"),
                  -dplyr::matches("\\bid\\b")) %>%
    list() %>%
    purrr::set_names(raw$waist_ui_type)

  return(raw_out)
}


#' Parse WTM targeting data (to list columns)
#'
#' @param col column that includes raw JSON
#' @param p used for iterator for progressr
#' @return returns a tibble
#' @export
wtm_parse_targeting2 <- function(col, p) {

  if(!missing(p)){
    p()
  }

  cols_to_add <- c("waist_custom_audience", "waist_ca_engagement_page",
                   "waist_ca_engagement_ig", "waist_ca_pixel", "waist_interest",
                   "waist_language", "waist_education_status", "waist_friends_connection",
                   "waist_connection", "waist_ca_lookalike", "waist_ca_engagement_video",
                   "waist_bct", "waist_employers", "waist_ca_datafile",
                   "waist_ca_engagement_event", "waist_school", "waist_job_titles",
                   "waist_dynamic_rule")

  if(is.na(col)){
    return(tibble(row_na = T) %>% add_cols(cols_to_add))
  }

  df_raw <- col %>%
    jsonlite::fromJSON() %>%
    purrr::map(wtm_parse_json2)

  df_rows <- df_raw %>%
    dplyr::select(dplyr::contains("waist"), dplyr::everything())

  return(df_rows)
}


add_cols <- function(data, cname, default = FALSE) {
  add <-cname[!cname%in%names(data)]

  if(length(add)!=0) data[add] <- default
  return(data)
}
