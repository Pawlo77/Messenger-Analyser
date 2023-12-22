


#####ui_emoji#####


ui_emoji <- fluidPage(
  
  titlePanel("Most common emojis"),
  fluidRow(
    column(2, 
           checkboxGroupInput("user_emoji", 
                              h5("Pokemon"),
                              choiceNames = c("Kiddo", "Misiu", "Pipi"),
                              choiceValues = c("Krzysiek Adamczyk","Michał Iwaniuk","Paweł Pozorski"),
                              selected = "Paweł Pozorski")
    ),
    column(2,
           checkboxGroupInput("group_emoji", 
                              h5("group"),
                              choiceNames = c("Tak","Nie"),
                              choiceValues = c(T,F),
                              selected = F)
    ),
    column(2, 
           checkboxGroupInput("words_emoji", 
                              h5("With word, behind/after"),
                              choices = c("word_behind", "word_next"),
                              selected = NULL)
           
    ),
    column(2,
           dateRangeInput("date_emoji", 
                          h5("Date range"),
                          start = "2016-06-01")
    ),
    column(2, 
           numericInput("n_emoji",
                        h5("emojis number"),
                        value = 5,
                        min = 1,
                        max=20)
           
    ),
  ),
  
  fluidRow(column(12,
                  plotlyOutput("barplot_emoji")
  )
  )
  
)

#####ui_time_respond#####

ui_time_respond <- fluidPage(
  titlePanel("Average response time"),
  fluidRow(
    column(4, 
           radioButtons("user_time_respond", 
                        h5("Pokemon"),
                        choiceNames = c("Kiddo", "Misiu", "Pipi"),
                        choiceValues = c("Krzysiek Adamczyk","Michał Iwaniuk","Paweł Pozorski"),
                        selected = "Paweł Pozorski")
    ),
    column(8,
           sliderInput("date_time_respond",
                       label = h5("Zakres dat"),
                       min = as.Date("2017-01-01"), 
                       max = as.Date("2024-01-01"),
                       value = c(as.Date("2017-01-01"), as.Date("2024-01-01")),
           )
    ),
    
  ),
  
  
  fluidRow(
    column(12,
           plotlyOutput("heatmap_time_respond")
    ),
  )
)

#####ui_count_messages####

ui_count_messages <- fluidPage(
  titlePanel("most messages with people"),
  fluidRow(
    column(2, 
           radioButtons("user_count_messages", 
                        h5("Pokemon"),
                        choiceNames = c("Kiddo", "Misiu", "Pipi"),
                        choiceValues = c("Krzysiek Adamczyk","Michał Iwaniuk","Paweł Pozorski"),
                        selected = "Paweł Pozorski")
    ),
    
    
    column(2,
           radioButtons("group_gender_count_messages", 
                        h5("group/gender"),
                        choiceNames = c("group","male","female"),
                        choiceValues = c("group","male","female"),
                        selected = "male")
           
    ),
    
    
    column(2,
           dateRangeInput("date_count_messages", 
                          h5("Date range"),
                          start = "2016-06-01")
    ),
    column(2, 
           numericInput("n_count_messages",
                        h5("conversations number"),
                        value = 5,
                        min = 1,
                        max=20)
    ),
  ),
  
  fluidRow(
    plotlyOutput("barplot_count_messages")
  )
)


#####server####

server <- function(input, output) {
  
  output$barplot_emoji <- renderPlotly({
    
    #####data_processing####
    emojis_number = input$n_emoji
    
    filtered_df <- combined_emoji %>% 
      filter(name %in% input$user_emoji,
             is_group %in% input$group_emoji,
             date >= input$date_emoji[1],
             date <= input$date_emoji[2])
    
    if("word_behind" %in% input$words_emoji && "word_next"%in% input$words_emoji){
      df <- filtered_df %>% 
        filter(!is.na(word_behind),!is.na(word_next),str_starts(word_next,"[a-z]|[A-Z]"),str_starts(word_behind,"[a-z]|[A-Z]"))%>%
        group_by(emoji,word_behind,word_next)%>%
        summarise(n = n())%>% ungroup() %>% 
        arrange(desc(n)) %>% slice(1:emojis_number) %>%
        mutate(emojis_combined = paste(word_behind,emoji,word_next)) %>%
        mutate(emojis_combined = fct_reorder(emojis_combined,-n))
      
    }
    else if("word_behind" %in% input$words_emoji){
      df <- filtered_df %>% 
        filter(!is.na(word_behind),str_starts(word_behind,"[a-z]|[A-Z]"))%>%
        group_by(emoji,word_behind)%>%
        summarise(n = n()) %>% ungroup() %>% 
        arrange(desc(n)) %>% slice(1:emojis_number) %>%
        mutate(emojis_combined = paste(word_behind,emoji))%>%
        mutate(emojis_combined = fct_reorder(emojis_combined,-n))
      
    }
    else if("word_next" %in% input$words_emoji) {
      df <- filtered_df %>% 
        filter(!is.na(word_next),str_starts(word_next,"[a-z]|[A-Z]"))%>%
        group_by(emoji,word_next)%>%
        summarise(n = n()) %>% 
        ungroup() %>% 
        arrange(desc(n)) %>% slice(1:emojis_number) %>%
        mutate(emojis_combined = paste(emoji,word_next)) %>%
        mutate(emojis_combined = fct_reorder(emojis_combined,-n))
      
    }
    else{
      df <- filtered_df %>% group_by(emoji)%>%
        summarise(n = n()) %>%
        arrange(desc(n)) %>% slice(1:emojis_number) %>%
        mutate(emojis_combined = fct_reorder(emoji,-n))
    }
    
    #####plot####
    plot_ly(df,x=~emojis_combined,y=~n,type="bar")%>%
      layout(xaxis = list(title = "",tickfont = list(size = 30))) %>%
      layout(xaxis = list(fixedrange = TRUE), yaxis = list(title = "liczba użyć",fixedrange = TRUE)) %>%
      config(displayModeBar = FALSE)
    
    
  })
  
  output$heatmap_time_respond <- renderPlotly({
    
    #####data_processing####
    filtered_df <- combined_time_respond %>%    #combined_time respond
      filter(name %in% input$user_time_respond,
             time_send >= input$date_time_respond[1],
             time_send <= input$date_time_respond[2])
    
    df <- filtered_df %>%
      group_by(day_send,round_hour_send) %>%
      summarise(mean_delta_min = round(mean(delta_min),digits = 2))%>%
      mutate(label = paste("Dzień:", day_send, "\nGodzina:", round_hour_send, "\nŚredni czas odpowiedzi:", mean_delta_min, "minut"))%>%
      mutate(log_delta = log10(mean_delta_min))
    
    df1 <- full_join(df, days_hours, by = join_by(day_send==Var1,round_hour_send==Var2 )) %>%
      mutate(label = case_when(
        is.na(label)~"Brak danych",
        T~label))
    
    
    #####plot####
    ggplotly(
      ggplot(df1,aes(x = round_hour_send, 
                     y=day_send, 
                     fill=log_delta, 
                     text=label)) + 
        geom_tile()+
        scale_fill_stepsn(colors = c("#FFFFD9", "#EDF8B1", "#C7E9B4", "#7FCDBB", "#41B6C4", "#1D91C0", "#225EA8", "#253494" ,"#081D58"),
                          breaks = c(0,0.7,1,1.18,1.3,1.48,1.78,2.08,2.7),
                          labels = c("1 min","5 min","15 min","10 min","20 min","30 min","60 min","120 min","500 min"))+
        theme_minimal()+
        theme(axis.text.x = element_text(angle = 50))+
        scale_x_discrete(breaks = time_sequence)+
        labs(x = "Godzina otrzymania wiadomości",
             y = "Dzień tygodnia",
             fill = "Sredni czas \n odpowiedzi")
      ,tooltip = "text") %>% 
      layout(xaxis = list(fixedrange = TRUE),
             yaxis = list(fixedrange = TRUE)) %>%
      config(displayModeBar = FALSE)
    
  })
  
  output$barplot_count_messages <- renderPlotly({
    
    conversations_number <- input$n_count_messages
    
    
    if(input$group_gender_count_messages == "group"){
      
      filtered_df <- combined_count_messages %>%
        filter(is_group == T,
               df_name == input$user_count_messages,
               date >= input$date_count_messages[1],
               date <= input$date_count_messages[2])
      
      df <- filtered_df %>% 
        group_by(conversation_id) %>%
        summarise(n = n()) %>%
        arrange(desc(n)) %>% 
        slice(1:conversations_number) %>%
        mutate(conversation_id = fct_reorder(conversation_id,-n))
      
      plot_ly(df,
              x=~conversation_id,
              y=~n,
              type = "bar") %>%
        layout(xaxis = list(fixedrange = TRUE), 
               yaxis = list(fixedrange = TRUE)) %>%
        config(displayModeBar = FALSE)
      
    }else{
      
      df2 <- combined_count_messages %>% 
        filter(is_group == FALSE,
               df_name == input$user_count_messages,
               name == input$user_count_messages,
               date_ >= input$date_count_messages[1],
               date_ <= input$date_count_messages[2]) %>% 
        group_by(conversation_id) %>% 
        summarise(MessegesSent = sum(min_messages_is_0)) %>% 
        ungroup() 
      df3 <- combined_count_messages %>% 
        filter(is_group == FALSE,
               df_name == input$user_count_messages,
               name != input$user_count_messages,
               gender == input$group_gender_count_messages,
               date_ >= input$date_count_messages[1], 
               date <= input$date_count_messages[2]) %>% 
        group_by(conversation_id, name, gender) %>% 
        summarise(MessegesReceived = sum(min_messages_is_0)) %>% 
        ungroup() 
      df <- left_join(df2,df3, by = c("conversation_id" = "conversation_id"))
      df <- df %>% mutate(n = MessegesSent + MessegesReceived)
      df <- df %>% 
        arrange(desc(n)) %>% 
        slice(1:conversations_number) %>%
        mutate(name = fct_reorder(name,-n))
      
      plot_ly(df,
              x=~name,
              y=~n,
              type = "bar") %>%
        layout(xaxis = list(fixedrange = TRUE), 
               yaxis = list(fixedrange = TRUE)) %>%
        config(displayModeBar = FALSE)
      
    }
    
    
    
  })
}

uiMain <- navbarPage(
  title = "Messenger Analyser",
  tabPanel("emoji", ui_emoji),
  tabPanel("countmessages", ui_count_messages),
  tabPanel("timie respond", ui_time_respond),
  
  
  
  theme = bslib::bs_theme(bootswatch = "cosmo"),
  
  
  header = tags$head(tags$link(rel = "stylesheet", href = "https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css"))
  
)

shinyApp(ui = uiMain, server = server)


