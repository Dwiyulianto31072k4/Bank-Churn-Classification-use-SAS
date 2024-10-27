/* DATA COLLECTION */
filename datafile '/shared/home/dwi.yulianto@student.umn.ac.id/casuser/data_credit_LGBank.csv';
proc import datafile=datafile
            out=mydataset 
            dbms=csv 
            replace; 
    getnames=yes; 
run;

proc print data=mydataset(obs=10);
run;


/* Eksplorasi Data */

/* Buat scatter plot menggunakan SGPLOT */
proc sgplot data=mydataset;
    title 'Correlation between Revenue and Trans Amount';
    scatter x=Revenue y=Trans_Amount / markerattrs=(symbol=circlefilled size=10);
    xaxis label='Revenue';
    yaxis label='Trans Amount';
run;


/* Buat box plot menggunakan SGPLOT */
proc sgplot data=mydataset;
    title 'Comparison of Customer Income Distribution by Churn';
    hbox Customer_Age / category=Attrition_Flag group=Income_Category 
                        discreteoffset=0.1 fillattrs=(transparency=0.6);
    xaxis label='Churn';
    yaxis label='Customer Age';
run;

/* DATA CLEANING */

/* Menghitung jumlah nilai yang hilang dan total pengamatan */
proc means data=mydataset n nmiss;
    output out=missing_summary nmiss= / autoname;
run;

/* Menghitung persentase nilai yang hilang */
data missing_percentage;
    set missing_summary;
    array nums _numeric_;
    Total_Observations = _FREQ_;
    drop _TYPE_ _FREQ_;
    do over nums;
        if Total_Observations > 0 then nums = (nums / Total_Observations) * 100;
        else nums = 0;
    end;
run;

/* Menampilkan hasil persentase nilai yang hilang */
proc print data=missing_percentage;
run;

/* Identifikasi Outlier */

/* Membuat makro untuk membuat boxplot */
%macro create_boxplots(data=, columns=);
    ods graphics on;
    %let n = %sysfunc(countw(&columns));
    %do i = 1 %to &n;
        %let col = %scan(&columns, &i);
        proc sgplot data=&data;
            vbox &col;
            title "Boxplot &col";
            yaxis label="Nilai"; 
        run;
    %end;
    ods graphics off;
%mend create_boxplots;

/* Daftar kolom numerik */
%let numeric_columns = Customer_Age Dependent_count Months_on_book Total_Relationship_Count 
                       Credit_Limit Months_Inactive Contacts_Count Total_Revolving_Bal Total_Trans_Ct
                       Avg_Open_To_Buy Avg_Utilization_Ratio Trans_Amount Revenue;

/* Memanggil makro untuk membuat boxplot untuk setiap kolom numerik */
%create_boxplots(data=mydataset, columns=&numeric_columns);

/* setelah tahu bahwa terdapat outlier pada beberapa kolom seperti: Credit_Limit, Months_Inactive, Contacts_Count, Total_Revolving_Bal, Avg_Open_To_Buy, Avg_Utilization_Ratio, Trans_Amount, Revenue maka kita perlu menghapusnya */

/* Menentukan kolom numerik yang akan dihapus outlier-nya */
%let outlier_columns = Credit_Limit Months_Inactive Contacts_Count Total_Revolving_Bal Avg_Open_To_Buy 
                       Avg_Utilization_Ratio Trans_Amount Revenue;

/* Menghitung IQR untuk setiap kolom numerik */
proc means data=mydataset q1 q3 noprint;
    var &outlier_columns;
    output out=iqr_summary q1= Credit_Limit_Q1 Months_Inactive_Q1 Contacts_Count_Q1 Total_Revolving_Bal_Q1
                           Avg_Open_To_Buy_Q1 Avg_Utilization_Ratio_Q1 Trans_Amount_Q1 Revenue_Q1
                 q3= Credit_Limit_Q3 Months_Inactive_Q3 Contacts_Count_Q3 Total_Revolving_Bal_Q3
                      Avg_Open_To_Buy_Q3 Avg_Utilization_Ratio_Q3 Trans_Amount_Q3 Revenue_Q3;
run;

/* Memastikan data iqr_summary dihasilkan */
proc print data=iqr_summary;
run;

/* Membuat makro untuk mengganti outlier dengan nilai batas (lower/upper bound) */
%macro replace_outliers_with_iqr(data=, columns=, iqr_data=);
    data &data;
        set &data;
        if _N_ = 1 then set &iqr_data;
        array cols[*] &columns;
        array q1s[*] Credit_Limit_Q1 Months_Inactive_Q1 Contacts_Count_Q1 Total_Revolving_Bal_Q1
                      Avg_Open_To_Buy_Q1 Avg_Utilization_Ratio_Q1 Trans_Amount_Q1 Revenue_Q1;
        array q3s[*] Credit_Limit_Q3 Months_Inactive_Q3 Contacts_Count_Q3 Total_Revolving_Bal_Q3
                      Avg_Open_To_Buy_Q3 Avg_Utilization_Ratio_Q3 Trans_Amount_Q3 Revenue_Q3;
        do i = 1 to dim(cols);
            col_q1 = q1s[i];
            col_q3 = q3s[i];
            col_iqr = col_q3 - col_q1;
            lower_bound = col_q1 - 1.5*col_iqr;
            upper_bound = col_q3 + 1.5*col_iqr;
            if cols[i] < lower_bound then cols[i] = lower_bound;
            else if cols[i] > upper_bound then cols[i] = upper_bound;
        end;
        drop i col_q1 col_q3 col_iqr lower_bound upper_bound;
    run;
%mend replace_outliers_with_iqr;

/* Memanggil makro untuk mengganti outlier dengan nilai batas (lower/upper bound) */
%replace_outliers_with_iqr(data=mydataset, columns=&outlier_columns, iqr_data=iqr_summary);

/* Menampilkan data setelah penggantian outlier */
proc print data=mydataset(obs=10);
run;

/* Membuat boxplot setelah penanganan outlier */
%create_boxplots(data=mydataset, columns=&numeric_columns);


/* MENYIMPAN DATA YANG SUDAH DIBERSIHKAN DI CSV BARU */

/* proc export data=mydataset
    outfile='/shared/home/dwi.yulianto@student.umn.ac.id/casuser/cleaned_data_credit_LGBank.csv'
    dbms=csv replace;
    putnames=yes; 
run;

