
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into lh_silver.departments as DBT_INTERNAL_DEST
      using departments__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.DepartmentID = DBT_INTERNAL_DEST.DepartmentID
          

      when matched then update set
         * 

      when not matched then insert *
