session server;

/* Start checking for existence of each input table */
exists0=doesTableExist("CASUSER(dwi.yulianto@student.umn.ac.id)", "CLEANED_DATA_CREDIT_LGBANK");
if exists0 == 0 then do;
  print "Table "||"CASUSER(dwi.yulianto@student.umn.ac.id)"||"."||"CLEANED_DATA_CREDIT_LGBANK" || " does not exist.";
  print "UserErrorCode: 100";
  exit 1;
end;
print "Input table: "||"CASUSER(dwi.yulianto@student.umn.ac.id)"||"."||"CLEANED_DATA_CREDIT_LGBANK"||" found.";
/* End checking for existence of each input table */


  _dp_inputTable="CLEANED_DATA_CREDIT_LGBANK";
  _dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

  _dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table                                           data */
data "f113d2a3-abfc-475b-8a6e-9327e9f6f1a1" (caslib="CASUSER(dwi.yulianto@student.umn.ac.id)" promote="no");


    /* Set the input                                                                set */
    set "CLEANED_DATA_CREDIT_LGBANK" (caslib="CASUSER(dwi.yulianto@student.umn.ac.id)"   drop="_TYPE_"n  drop="_FREQ_"n  drop="Credit_Limit_Q1"n  drop="Months_Inactive_Q1"n  drop="Contacts_Count_Q1"n  drop="Total_Revolving_Bal_Q1"n  drop="Avg_Open_To_Buy_Q1"n  drop="Avg_Utilization_Ratio_Q1"n  drop="Trans_Amount_Q1"n  drop="Revenue_Q1"n  drop="Credit_Limit_Q3"n  drop="Months_Inactive_Q3"n  drop="Contacts_Count_Q3"n  drop="Total_Revolving_Bal_Q3"n  drop="Avg_Open_To_Buy_Q3"n  drop="Avg_Utilization_Ratio_Q3"n  drop="Trans_Amount_Q3"n  drop="Revenue_Q3"n );

/* END data step                                                                    run */
run;
';
if rc.statusCode != 0 then do;
  print "Error executing datastep";
  exit 2;
end;
  _dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

  _dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

/* BEGIN statement a896c902_5138_11e7_b114_b2f933d5fe66          casl */
_dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";
_dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
_dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";
_dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
    all_columns = columnList(_dp_inputCaslib, _dp_inputTable);
input_columns={"GENDER"};
/* To replace column instead of creating a new one, replace the following line of code with */
/* copyvars_columns=all_columns-input_columns; */
copyvars_columns=all_columns;

dataPreprocess.catTrans status=rc /
    table={name=_dp_inputTable, caslib=_dp_inputCaslib}
    casout={name=_dp_outputTable, caslib=_dp_outputCaslib, promote=0, replace=1}
    inputs=input_columns
    method="ONEHOT"
    copyVars=copyvars_columns
    outVarsNamePrefix="ENC"
;
if rc.statusCode != 0 then do;
    print "Error running one-hot encoding action in CASL";
    exit 3;
end;




/* If the status code of the last action run is unsuccessful, then we should exit */
if _status.statusCode != 0 then do;
  exit 1;
end;
/* END statement a896c902_5138_11e7_b114_b2f933d5fe66            casl */
  _dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

  _dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

/* BEGIN statement a896c902_5138_11e7_b114_b2f933d5fe66          casl */
_dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";
_dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
_dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";
_dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
    all_columns = columnList(_dp_inputCaslib, _dp_inputTable);
input_columns={"EDUCATION_LEVEL"};
/* To replace column instead of creating a new one, replace the following line of code with */
/* copyvars_columns=all_columns-input_columns; */
copyvars_columns=all_columns;

dataPreprocess.catTrans status=rc /
    table={name=_dp_inputTable, caslib=_dp_inputCaslib}
    casout={name=_dp_outputTable, caslib=_dp_outputCaslib, promote=0, replace=1}
    inputs=input_columns
    method="ONEHOT"
    copyVars=copyvars_columns
    outVarsNamePrefix="ENC"
;
if rc.statusCode != 0 then do;
    print "Error running one-hot encoding action in CASL";
    exit 3;
end;




/* If the status code of the last action run is unsuccessful, then we should exit */
if _status.statusCode != 0 then do;
  exit 1;
end;
/* END statement a896c902_5138_11e7_b114_b2f933d5fe66            casl */
  _dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

  _dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

/* BEGIN statement a896c902_5138_11e7_b114_b2f933d5fe66          casl */
_dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";
_dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
_dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";
_dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
    all_columns = columnList(_dp_inputCaslib, _dp_inputTable);
input_columns={"MARITAL_STATUS"};
/* To replace column instead of creating a new one, replace the following line of code with */
/* copyvars_columns=all_columns-input_columns; */
copyvars_columns=all_columns;

dataPreprocess.catTrans status=rc /
    table={name=_dp_inputTable, caslib=_dp_inputCaslib}
    casout={name=_dp_outputTable, caslib=_dp_outputCaslib, promote=0, replace=1}
    inputs=input_columns
    method="ONEHOT"
    copyVars=copyvars_columns
    outVarsNamePrefix="ENC"
;
if rc.statusCode != 0 then do;
    print "Error running one-hot encoding action in CASL";
    exit 3;
end;




/* If the status code of the last action run is unsuccessful, then we should exit */
if _status.statusCode != 0 then do;
  exit 1;
end;
/* END statement a896c902_5138_11e7_b114_b2f933d5fe66            casl */
  _dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

  _dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

/* BEGIN statement a896c902_5138_11e7_b114_b2f933d5fe66          casl */
_dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";
_dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
_dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";
_dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
    all_columns = columnList(_dp_inputCaslib, _dp_inputTable);
input_columns={"CARD_CATEGORY"};
/* To replace column instead of creating a new one, replace the following line of code with */
/* copyvars_columns=all_columns-input_columns; */
copyvars_columns=all_columns;

dataPreprocess.catTrans status=rc /
    table={name=_dp_inputTable, caslib=_dp_inputCaslib}
    casout={name=_dp_outputTable, caslib=_dp_outputCaslib, promote=0, replace=1}
    inputs=input_columns
    method="ONEHOT"
    copyVars=copyvars_columns
    outVarsNamePrefix="ENC"
;
if rc.statusCode != 0 then do;
    print "Error running one-hot encoding action in CASL";
    exit 3;
end;




/* If the status code of the last action run is unsuccessful, then we should exit */
if _status.statusCode != 0 then do;
  exit 1;
end;
/* END statement a896c902_5138_11e7_b114_b2f933d5fe66            casl */
  _dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

  _dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

/* BEGIN statement a896c902_5138_11e7_b114_b2f933d5fe66          casl */
_dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";
_dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
_dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";
_dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
    all_columns = columnList(_dp_inputCaslib, _dp_inputTable);
input_columns={"INCOME_CATEGORY"};
/* To replace column instead of creating a new one, replace the following line of code with */
/* copyvars_columns=all_columns-input_columns; */
copyvars_columns=all_columns;

dataPreprocess.catTrans status=rc /
    table={name=_dp_inputTable, caslib=_dp_inputCaslib}
    casout={name=_dp_outputTable, caslib=_dp_outputCaslib, promote=0, replace=1}
    inputs=input_columns
    method="ONEHOT"
    copyVars=copyvars_columns
    outVarsNamePrefix="ENC"
;
if rc.statusCode != 0 then do;
    print "Error running one-hot encoding action in CASL";
    exit 3;
end;




/* If the status code of the last action run is unsuccessful, then we should exit */
if _status.statusCode != 0 then do;
  exit 1;
end;
/* END statement a896c902_5138_11e7_b114_b2f933d5fe66            casl */
  _dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

  _dp_outputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

dataStep.runCode result=r status=rc / code='/* BEGIN data step with the output table                                           data */
data "f113d2a3-abfc-475b-8a6e-9327e9f6f1a1" (caslib="CASUSER(dwi.yulianto@student.umn.ac.id)" promote="no");

    length
       "Gender"n 8
       "Education_Level"n 8
       "Marital_Status"n 8
       "Card_Category"n 8
       "Income_Category"n 8
    ;

    /* Set the input                                                                set */
    set "f113d2a3-abfc-475b-8a6e-9327e9f6f1a1" (caslib="CASUSER(dwi.yulianto@student.umn.ac.id)"   drop="Gender"n  drop="Education_Level"n  drop="Marital_Status"n  drop="Income_Category"n  drop="Card_Category"n  rename=("ENC_Gender"n = "Gender"n)  rename=("ENC_Education_Level"n = "Education_Level"n)  rename=("ENC_Marital_Status"n = "Marital_Status"n)  rename=("ENC_Card_Category"n = "Card_Category"n)  rename=("ENC_Income_Category"n = "Income_Category"n) );

/* END data step                                                                    run */
run;
';
if rc.statusCode != 0 then do;
  print "Error executing datastep";
  exit 2;
end;
  _dp_inputTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
  _dp_inputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

  _dp_outputTable="DATA_BANK_FINAL";
  _dp_outputCaslib="CASUSER(dwi.yulianto@student.umn.ac.id)";

srcCasTable="f113d2a3-abfc-475b-8a6e-9327e9f6f1a1";
srcCasLib="CASUSER(dwi.yulianto@student.umn.ac.id)";
tgtCasTable="DATA_BANK_FINAL";
tgtCasLib="CASUSER(dwi.yulianto@student.umn.ac.id)";
saveType="csv";
tgtCasTableLabel="";
replace=1;
saveToDisk=1;

exists = doesTableExist(tgtCasLib, tgtCasTable);
if (exists !=0) then do;
  if (replace == 0) then do;
    print "Table already exists and replace flag is set to false.";
    exit ({severity=2, reason=5, formatted="Table already exists and replace flag is set to false.", statusCode=9});
  end;
end;

if (saveToDisk == 1) then do;
  /* Save will automatically save as type represented by file ext */
  saveName=tgtCasTable;
  if(saveType != "") then do;
    saveName=tgtCasTable || "." || saveType;
  end;
  table.save result=r status=rc / caslib=tgtCasLib name=saveName replace=replace
    table={
      caslib=srcCasLib
      name=srcCasTable
    };
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
  tgtCasPath=dictionary(r, "name");

  dropTableIfExists(tgtCasLib, tgtCasTable);
  dropTableIfExists(tgtCasLib, tgtCasTable);

  table.loadtable result=r status=rc / caslib=tgtCasLib path=tgtCasPath casout={name=tgtCasTable caslib=tgtCasLib} promote=1;
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
end;

else do;
  dropTableIfExists(tgtCasLib, tgtCasTable);
  dropTableIfExists(tgtCasLib, tgtCasTable);
  table.promote result=r status=rc / caslib=srcCasLib name=srcCasTable target=tgtCasTable targetLib=tgtCasLib;
  if rc.statusCode != 0 then do;
    return rc.statusCode;
  end;
end;


dropTableIfExists("CASUSER(dwi.yulianto@student.umn.ac.id)", "f113d2a3-abfc-475b-8a6e-9327e9f6f1a1");

function doesTableExist(casLib, casTable);
  table.tableExists result=r status=rc / caslib=casLib table=casTable;
  tableExists = dictionary(r, "exists");
  return tableExists;
end func;

function dropTableIfExists(casLib,casTable);
  tableExists = doesTableExist(casLib, casTable);
  if tableExists != 0 then do;
    print "Dropping table: "||casLib||"."||casTable;
    table.dropTable result=r status=rc/ caslib=casLib table=casTable quiet=0;
    if rc.statusCode != 0 then do;
      exit();
    end;
  end;
end func;

/* Return list of columns in a table */
function columnList(casLib, casTable);
  table.columnInfo result=collist / table={caslib=casLib,name=casTable};
  ndimen=dim(collist['columninfo']);
  featurelist={};
  do i =  1 to ndimen;
    featurelist[i]=upcase(collist['columninfo'][i][1]);
  end;
  return featurelist;
end func;
