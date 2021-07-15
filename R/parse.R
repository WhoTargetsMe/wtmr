#' Parse WTM targeting data
#'
#' @param col column that includes raw JSON
#' @p used for iterator for progressr
#' @return returns a tibble
#' @export
wtm_parse_targeting <- function(col, p) {

  if(!missing(p)){
    p()
  }

  cols_to_add <- c("waist_custom_audience", "waist_engagement_page",
                   "waist_engagement_ig", "waist_pixel", "waist_interest",
                   "waist_language", "waist_education", "waist_friends_connection",
                   "waist_connection", "waist_lookalike", "waist_engagement_video",
                   "waist_bct", "waist_employers", "waist_datafile",
                   "waist_engagement_event", "waist_school", "waist_job_titles",
                   "waist_dynamic_rule")

  if(is.na(col)){
    return(tibble(row_na = T) %>% add_cols(cols_to_add))
  }

  df_cols <- col %>%
    jsonlite::fromJSON() %>%
    purrr::map_dfc(wtm_parse_json) %>%
    dplyr::select(contains("waist"), dplyr::everything())  %>%
    add_cols(cols_to_add) %>%
    dplyr::mutate(waist_custom_audience = ifelse(waist_pixel | waist_engagement_page | waist_engagement_ig | waist_engagement_video | waist_datafile | waist_lookalike | waist_engagement_event, T, F)) %>%
    dplyr::mutate(waist_employment = ifelse(waist_job_titles | waist_employers, T, F))


  return(df_cols)
}

#' Parse WTM targeting jSON data
#'
#' @param raw raw JSON
#' @return returns a tibble
#' @export
wtm_parse_json <- function(raw) {

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

  } else if(raw$waist_ui_type == "INTERESTS"){
    raw_out <- raw %>%
      purrr::pluck("interests") %>%
      dplyr::summarise(waist_interests = paste0(name, collapse = "|||"),
                       waist_interests_id = paste0(id, collapse = "|||")) %>%
      dplyr::mutate(waist_interest = T)
  } else if(raw$waist_ui_type == "LOCALE"){
    raw_out <- raw %>%
      purrr::pluck("serialized_data") %>%
      jsonlite::fromJSON() %>%
      dplyr::bind_cols() %>%
      dplyr::mutate(languages = raw$locales) %>%
      dplyr::summarise(waist_languages = paste0(languages, collapse = "|||"),
                       waist_languages_id = paste0(locales, collapse = "|||")) %>%
      dplyr::mutate(waist_language = T)
  } else if(raw$waist_ui_type == "ED_STATUS"){
    raw_out <- raw  %>%
      purrr::pluck("serialized_data") %>%
      jsonlite::fromJSON() %>%
      dplyr::bind_cols() %>%
      dplyr::mutate(education_status = raw$edu_status) %>%
      dplyr::summarise(waist_edu_status = paste0(education_status, collapse = "|||"),
                       waist_edu_status_id = paste0(edu_status, collapse = "|||")) %>%
      dplyr::mutate(waist_education = T)
  } else if(raw$waist_ui_type == "EDU_SCHOOLS"){
    raw_out <- raw  %>%
      purrr::pluck("serialized_data") %>%
      jsonlite::fromJSON() %>%
      dplyr::bind_cols(dplyr::bind_cols(raw)) %>%
      dplyr::summarise(waist_schools = paste0(school_names, collapse = "|||"),
                       waist_schools_id = paste0(school_ids, collapse = "|||")) %>%
      dplyr::mutate(waist_school = T)
  } else if(stringr::str_detect(raw$waist_ui_type, "CUSTOM_AUDIENCES")){
    raw_out <- raw  %>%
      rlist::list.flatten() %>%
      purrr::pluck("serialized_data") %>%
      jsonlite::fromJSON() %>%
      dplyr::bind_cols(dplyr::bind_cols(rlist::list.flatten(raw))) %>%
      janitor::clean_names() %>%
      dplyr::rename_at(dplyr::vars(dplyr::contains(c("business_id", "ca_fbid", "ca_type", "current_time_string", "is_opted_out", "is_unhandled_dfca", "upload_time_string"))),
                       ~paste0(.x, "_", raw$waist_ui_type))


    if(raw$waist_ui_type == "CUSTOM_AUDIENCES_WEBSITE"){
      raw_out <- raw_out %>%
        dplyr::mutate(waist_pixel = T)
    } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_ENGAGEMENT_PAGE"){
      raw_out <- raw_out %>%
        dplyr::mutate(waist_engagement_page = T)
    } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_ENGAGEMENT_IG"){
      raw_out <- raw_out %>%
        dplyr::mutate(waist_engagement_ig = T)
    } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_LOOKALIKE"){
      raw_out <- raw_out %>%
        dplyr::mutate(waist_lookalike = T)
    } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_ENGAGEMENT_VIDEO"){
      raw_out <- raw_out %>%
        dplyr::mutate(waist_engagement_video = T)
    } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_DATAFILE"){
      raw_out <- raw_out %>%
        dplyr::mutate(waist_datafile = T)
    } else if(raw$waist_ui_type == "CUSTOM_AUDIENCES_ENGAGEMENT_EVENT"){
      raw_out <- raw_out %>%
        dplyr::mutate(waist_engagement_event = T)
    }



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
                  -dplyr::contains("business_id_custom_audiences_engagement"),
                  -dplyr::matches("\\bid\\b"))

  return(raw_out)
}

add_cols <- function(data, cname, default = FALSE) {
  add <-cname[!cname%in%names(data)]

  if(length(add)!=0) data[add] <- F
  return(data)
}
