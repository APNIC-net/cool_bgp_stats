library(shiny)

# This code runs once each time the app is launched
shinyServer(function(input, output) {
  #This code runs once each time a user visits the app
  output$resCount <- renderPlot({
  #THis time runs once each time a user changes a value in a widget
    axis_text = ''
    if(input$radioResourceType == 'asn'){
      axis_text = 'ASNs'
    }else if(input$radioResourceType == 'ipv4'){
      axis_text = 'IPv4 Prefixes'
    }else{
      axis_text = 'IPv6 Prefixes'  
    }
    
    current_df = subset(delegated_df, delegated_df$resource_type == input$radioResourceType & delegated_df$cc == input$countrySelect & delegated_df$alloc_assig == input$assig_or_alloc)
    if(nrow(current_df) == 0){
      print('There are no resources that meet the specified criteria.')
      #TODO ver como abrir pop-up con mensaje
    }else{
      aggr = aggregate(resource_type ~ month + year, data=current_df, FUN = NROW)
      
      p <- ggplot(aggr, aes(month, resource_type))+
        stat_summary(fun.y = sum, geom = "bar") +
        scale_y_continuous(breaks = seq(1, max(aggr$resource_type))) +
        scale_x_date(
          labels = date_format("%Y-%m"),
          date_breaks = "1 month") +
        xlab('') + ylab(paste(c('Number of delegations of ', axis_text, ' per month'), sep='', collapse = ''))
      
  #      geom_text_repel(aes(label=Country))+ guides(colour=FALSE, size=F)+
   #     theme(legend.position='bottom')
      print(p)
    }
  })
})