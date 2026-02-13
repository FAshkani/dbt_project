
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into lh_gold.dim_products as DBT_INTERNAL_DEST
      using dim_products__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.ProductID = DBT_INTERNAL_DEST.ProductID
          

      when matched then update set
         * 

      when not matched then insert *
