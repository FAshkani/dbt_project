
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into lh_silver.territories as DBT_INTERNAL_DEST
      using territories__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.TerritoryID = DBT_INTERNAL_DEST.TerritoryID
          

      when matched then update set
         * 

      when not matched then insert *
