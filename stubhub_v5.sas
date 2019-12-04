%macro sh_auth(sh_base64appkey=, user=, pwd=);

	%let sh_tokenurl=https://api.stubhub.com/sellers/oauth/accesstoken?grant_type=client_credentials;

	filename resp TEMP;

	%let user=%sysfunc(quote(&user));
	%let pwd=%sysfunc(quote(&pwd));

	proc http
		url="&sh_tokenurl"
	    in=%unquote(%nrbquote('{"username":&user,"password":&pwd}'))
	    method="POST"
	    out=resp;
	    headers "Authorization"="Basic &sh_base64appkey" "Content-Type"="application/json" ;
	run;

	libname auth JSON fileref=resp;

	%global access_token;
	proc sql noprint;
		select value into: access_token from auth.alldata where p1="access_token";
	quit;

	libname auth clear;

%mend;

%macro sh_event_search(access_token=, searchName=, searchState=, searchVenueConfigId=, searchRows=500, lib=work);
	
	%let sh_eventsearch=https://api.stubhub.com/sellers/search/events/v3?name=&searchName%nrstr(&)state=&searchState%nrstr(&)VenueConfigId=&searchVenueConfigId%nrstr(&)rows=&searchRows;

	proc http
		url="&sh_eventsearch"
	    method="GET"
	    out=resp;
	    headers "Authorization"="Bearer &access_token" "Content-Type"="application/json" ;
	run;

	libname e JSON fileref=resp;

	data &lib..events;
		merge e.events e.events_ticketinfo e.events_venue(rename=(id=venueId name=venueName));
		by ordinal_events;
		drop ordinal:;
	run;

	libname e clear;

%mend;

%macro processListings(access_token=, sh_event_id=, lib=, looprows=250);

	%let sh_eventinventory=https://api.stubhub.com/sellers/find/listings/v3/?eventId=&sh_event_id;

	proc datasets lib=&lib nolist;
		delete _&sh_event_id;
	quit;

	filename resp TEMP;

	proc http CLEAR_CACHE
		url="&sh_eventinventory.%nrstr(&)start=0"
	    method="GET"
	    out=resp;
	    headers "Authorization"="Bearer &access_token" "Content-Type"="application/json";
		*debug level=2;
	run;

	libname i JSON fileref=resp;

	proc sql noprint;
		select totalListings into: totalListings from i.root;
	quit;

	%let totalLoops = %sysfunc(ceil(&totalListings/&looprows));

	libname i clear;

	%let start=0;
	%do i=1 %to &totalLoops;

		proc http CLEAR_CACHE
			url="&sh_eventinventory.%nrstr(&)start=&start%nrstr(&)rows=&looprows"
		    method="GET"
		    out=resp;
		    headers "Authorization"="Bearer &access_token" "Content-Type"="application/json";
		run;
		
		libname i JSON fileref=resp;

		data work.inventory;
			retain event_id &sh_event_id facevalue .;
			length sellerSectionName $ 100 sectionName $ 100 zone $ 50 row $ 20 seat $ 20 splitOption $ 10 splitQuantity $ 10  listingType $ 32;
			merge i.listings 
				  %if %sysfunc(exist(i.listings_facevalue)) %then %do; i.listings_facevalue(rename=(amount=facevalue)) %end;
				  i.listings_products 
				  i.listings_priceperproduct(rename=(amount=price));
			by ordinal_listings;
			drop ordinal:;
		run;

		proc append base=&lib.._&sh_event_id data=work.inventory force;
		run;
		
		libname i clear;

		%let start=%eval(&start + &looprows);
	%end;

	proc datasets lib=work nolist;
		delete inventory;
	quit;

%mend;

%macro concatevents(lib=, dsn=work.inventory);

	proc sql noprint;
		select memname into: datasets separated by ' ' from dictionary.tables where upcase(libname)=upcase("&lib");
	quit;
	
	%let tablecount = %sysfunc(countw(%superq(datasets),%str( )));

	%do i=1 %to &tablecount;
		%let table = %scan(&datasets, &i, %str( ));
		
		%if %substr(&table, 1, 1) eq _ %then %do;
			proc append base=&dsn. data=&lib..&table force;
			run;
		%end;

	%end;

%mend;

/**************************************** USAGE ************************************************/

/* 1) Authenticate with Stubhub. This sets the macro variable access_token */
%sh_auth(sh_base64appkey=Q3RIZnZrU0I4MXVvN09vQkNqQ2ZKZ1hyYjZSR3Q2ajM6VnFoYVlRa21vZzR0VWNvag==, user=matthew.perry@sas.com, pwd=matt8811);
%put &=access_token;

/* 2) Search for events. Produces a dataset names events in the specified library */
libname testrun "C:\Users\maperr\OneDrive - SAS\CBU\Stubhub\testrun";
%sh_event_search(access_token=&access_token, searchName=Twins, searchState=MN, searchVenueConfigId=345892, lib=testrun);

options nomprint;
/* 3) Iterate over the events to process ticket listings */
data _null_;
	set testrun.events;
	macro_call = cats('%nrstr(%processListings(access_token=&access_token, lib=&lib, sh_event_id=',id,'))');
	CALL EXECUTE(macro_call);
run;

/* 4) Concat the datasets created in step 3 */
%concatevents(lib=testrun, dsn=testrun.inventory);

/* 1A Combined process into one */
%macro sh_full_run(sh_base64appkey=,user=, pwd=, searchName=, searchState=, searchVenueConfigId=, lib=work);

	%sh_auth(sh_base64appkey=&sh_base64appkey, user=&user, pwd=&pwd);
	%sh_event_search(access_token=&access_token, searchName=&searchName, searchState=&searchState, searchVenueConfigId=&searchVenueConfigId, lib=&lib);
	data _null_;
		set testrun.events;
		macro_call = cats('%nrstr(%processListings(access_token=&access_token, lib=&lib, sh_event_id=',id,'))');
		CALL EXECUTE(macro_call);
	run;
	%concatevents(lib=&lib, dsn=&lib..inventory);

%mend;
libname rangers "C:\Users\maperr\OneDrive - SAS\CBU\Stubhub\Rangers";
%sh_full_run(sh_base64appkey=Q3RIZnZrU0I4MXVvN09vQkNqQ2ZKZ1hyYjZSR3Q2ajM6VnFoYVlRa21vZzR0VWNvag==, 
		     user=matthew.perry@sas.com, 
			 pwd=matt8811,
			 searchName=Rangers, 
			 searchState=TX, 
			 lib=rangers);
