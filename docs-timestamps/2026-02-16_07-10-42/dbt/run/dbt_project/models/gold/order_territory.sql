
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into lh_gold.order_territory as DBT_INTERNAL_DEST
      using order_territory__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.SalesOrderID = DBT_INTERNAL_DEST.SalesOrderID
          

      when matched then update set
         * 

      when not matched then insert *
