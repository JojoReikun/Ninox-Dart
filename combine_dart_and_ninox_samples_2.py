"""Script goal: make a csv file which combined the Ninox data for Koala genetic samples with the DART data for the same samples, 
with the main focus of having an overview of whihc Ninox sample belongs to which DArT order.
Folder name of DArT orders: DKo21-5970, where 21 is the year, 5970 is a 4digit number.
Files in the DArT order folder:
Report_DKo21-5970_moreOrders_SNP.csv or rather Report_DKo21-5970_*.csv
newer DArT orders also contain a file called SampleFile_DKo21-5970_*.csv, which contains the following columns:
    "PlateID","Row","Column","Organism","Species","Genotype","Tissue","Comments"

The ninox data involves two tables in Dog Surveys: 4-Genetic and 5-Extractions.
The DArT data is complex, contains multiple headers. 
    The sample names in the DArT report file can be found in the header row 7 from column 23 ("W") to the end.

Script functionality:
- iterate through all DArT order folders, determine which ones have a SampleFile, which ones don't.
- iterate through all the Report files to extract all available sample names, 
    write them to a new data sheet containing DArT order number, DArT file name and all sample files of that DArT file.
- read in the ninox data file for all current samples.

The final combined file should contain the following columns, potentially more if needed along the way:
"Project", "Council", "Sample.Name", "Sample.ID", "Latitude", "Longitude", "Survey.Type", "Extraction.Method", "Date.Sample", 
"Date.Extraction", "Tissue.Type", "DArT order number", "Genotype", "Comments"
"""

import os
import pandas as pd
import numpy as np
import glob
from inputimeout import inputimeout, TimeoutOccurred
import time
import threading
import json
import logging

"""
DArT folder structure - example:
DArT
- DKo21-5970
-- Report_DKo21-5970_*.csv
-- SampleFile_DKo21-5970_*.csv
- DKo21-8765
-- Report_DKo21-8765_*.csv
-- SampleFile_DKo21-8765_*.csv
"""


################## MAIN FUNCTIONS & CLASSES ##################
ninox_filedict = {"Dog": {"Genetics": "4 - Genetics.csv", "Extractions": "5 - Extractions.csv"}, 
                                "Drone": {"Genetics": "6 - Drone Genetics.csv", "Extractions": "7 - Drone Extractions.csv"}, 
                                "Opportunistic": {"Genetics":"9 - Opportunistic Genetics.csv", "Extractions": "9b - Opportunistic Extractions.csv"}, 
                                "Tracking": {"Genetics": "2 - TK Genetics.csv", "Extractions": "3 - TK Extractions.csv"}, 
                                "Partner": {"Genetics": "Partners Genetic data.csv", "Extractions": "Partner Extraction.csv"}}

class ninox_all():
    def __init__(self, ninox_filedict) -> None:
        self.ninox_data_status = False
        self.ninox_merged_currency = np.nan    
        self.ninox_data = pd.DataFrame()
        self.ninox_filedict = ninox_filedict
        pass

    def determine_data_status(self):
        # test if the ninox merged data file exists, if it does set data status to True:
        if os.path.isfile(os.getcwd() + f"/ninox_merged/ninox_merged.csv"):
            self.ninox_data_status = True
        else:
            self.ninox_data_status = False
        return 
    
    def test_currency(self):
        """This functions finds the newest sample date in the ninox_merged file and saves it to self.ninox_merged_currency. 
        It will be used to compare the actuality of the ninox data and the DArT data to determine if 
        new data needs to be downloaded."""
        if self.ninox_data_status == True:
            # read in the merged ninox data:
            self.ninox_data = pd.read_csv(os.getcwd() + f"/ninox_merged/ninox_merged.csv")
            # convert the Survey.Date column to datetime format, date format in Ninox data: DD/MM/YYYY:
            self.ninox_data["Survey.Date"] = pd.to_datetime(self.ninox_data["Survey.Date"], dayfirst=True).dt.date
            # find newest sample date in self.ninox_data
            self.ninox_merged_currency = self.ninox_data["Survey.Date"].dropna().max()
            print("ninox_currency, newest survey date: ", self.ninox_merged_currency)
        else:
            self.ninox_merged_currency = np.nan

        return self.ninox_merged_currency
    
    def merge_ninox_data_all(self):     
        """This function merges all ninox_merged_survey_type.csv files into one ninox_merged.csv file.
        If this is the first time (data status is False) the ninox data is being merged, then a new ninox_merged file is created.
        If this is not the first time (data status is True) the ninox data is being merged, then the ninox_merged file is appended to, 
        only the samples with Sample.Date newer than ninox_merged_currency are added from each survey type.
        The data status is determined by the existence of the ninox_merged file.
        """
        file_path = os.path.join(os.getcwd(), "ninox_merged", f"ninox_merged.csv")

        # load all ninox_merged_survey_type.csv files into a list of dataframes:
        ninox_merged_list = []

        if self.ninox_merged_currency is not np.nan:
            # extract all samples newer than currency for each survey type:
            for survey_type in self.ninox_filedict:
                # print survey type to console:
                print(f"survey type: {survey_type}")
                # read in the ninox_merged_survey_type.csv file, parse the date Survey.Date as DD/MM/YYYY format:
                ninox_merged_survey_type = pd.read_csv(os.getcwd() + f"/ninox_merged/ninox_merged_{survey_type}.csv", parse_dates=["Survey.Date"])
                # convert the Survey.Date column from datetime to date format, date format in Ninox data: DD/MM/YYYY:
                ninox_merged_survey_type["Survey.Date"] = ninox_merged_survey_type["Survey.Date"].dt.date
                # extract all samples newer than currency:
                ninox_merged_survey_type = ninox_merged_survey_type[ninox_merged_survey_type["Survey.Date"] > self.ninox_merged_currency]
                # append ninox_merged_survey_type to ninox_merged_list:
                ninox_merged_list.append(ninox_merged_survey_type)

        else:
            # read in and keep the entire ninox_merged_survey_type.csv file:
            for survey_type in self.ninox_filedict:
                # read in the ninox_merged_survey_type.csv file:
                ninox_merged_survey_type = pd.read_csv(os.getcwd() + f"/ninox_merged/ninox_merged_{survey_type}.csv")
                # append ninox_merged_survey_type to ninox_merged_list:
                ninox_merged_list.append(ninox_merged_survey_type)

        # concatenate all dataframes in ninox_merged_list:
        self.ninox_data = pd.concat(ninox_merged_list, ignore_index=True)
        # print shape of self.ninox_data:
        print("shape of self.ninox_data (all survey types concatenated): ", self.ninox_data.shape)

        # if the data status is true, append the new data to the existing ninox_merged.csv file:
        if self.ninox_data_status == True:
            # read in the existing ninox_merged.csv file:
            ninox_merged = pd.read_csv(os.getcwd() + f"/ninox_merged/ninox_merged.csv")
            # append self.ninox_data to ninox_merged:
            ninox_merged = pd.concat([ninox_merged, self.ninox_data], ignore_index=True)
            # save ninox_merged to file_path:
            ninox_merged.to_csv(file_path, index=False)
            # print number of new samples added per survey type and shape of new ninox_merged to log file:
            logging.info(f"number of new samples added: {self.ninox_data.shape[0]}, shape of new ninox_merged: {ninox_merged.shape}")
            overwrite = "appended"

        else:
            self.ninox_data.to_csv(file_path, index=False)
            # print file was saved:
            print("ninox_merged.csv was saved.")
            overwrite = "initial file created"

        # write ninox data merged and saved to file to log file, include overwrite status:
        logging.info(f"ninox data merged and saved to file, overwrite_status: {overwrite}.")

        return
    


class ninox_survey():
    """class to handle all operations for the ninox data of individual surveys.
    There are multipple sources for the Ninox data, which each have to be combined individually.
    Dog Surveys: 4- Genetics and 5 - Extractions.
    Drone Surveys: 6 - Drone Genetics and 7 - Drone Extractions, and also 9 - Opportunistic Genetics and 9b - Opportunistic Extractions.
    Tracking Surveys: 2 - TK Genetics and 3 - TK Extractions.
    and finally the Partner Genetics, with only Partner Extractions as a subtab.
    The modules in this class are there to read in the Genetics and the Extractions data respectively and finally combine the two for each survey type.    
    """
    def __init__(self, ninox_filedict, survey_type=np.nan, ninox_all_currency=np.nan) -> None:
        """
        self.ninox_currency holds the newest sample date from the merged ninox file, if that's already available, otherwise it's np.nan by default.
        A dataframe is created to hold data from the ninox files using the needed columns.
        self.skipped_columns holds all columns which are not in the Genetics file and remain to be read in from Extractions file.
        self.skipped_samples holds the sample names of the ones that were found in Extractions but not in ninox_data.
        self.currency describes the currency (newest sample) of the merged_ninox_survey_type file if it already exists."""
        self.currency = ninox_all_currency
        self.genetics_currency = np.nan
        self.extractions_currency = np.nan
        self.needed_columns = ["Project", "Sample.Name", "Genetic.ID", "Latitude", "Longitude", "Survey.Type", "Survey.Date", "Date.Extraction", "Extraction.Method", "Dart.Sample.ID", "Dart.Order.Number", "Extraction.ID"]   
        self.extra_columns = ["Council", "Scat.ID"] # columns which are only relevant for Dog Surveys. Set NaN for all other survey_types.
        self.ninox_genetics = pd.DataFrame()
        self.ninox_extractions = pd.DataFrame()
        self.ninox_remerge_survey = False
        self.remaining_columns = []
        self.skipped_samples = []
        self.ninox_filedict = ninox_filedict
        self.survey_type = survey_type
        print(f"handling survey type {self.survey_type} of ninox data ...")
        # print to log file that now the ninox data will be handled:
        logging.info(f"now the ninox data will be handled for survey type: {self.survey_type}.")
        pass
       
    def read_Genetics(self):
        # TODO: define this in subclass for each Survey Type because field names are called differently!!!!
        # refer to GPS_Harmonizer as reference.
        """This function reads in the "Genetics.csv" file from the ninox folder of the respective survey type and returns a pandas dataframe
        with all columns from all needed apparent in this file. To do so all column names are read in and compared to the
        needed column names. If a column name is found, the column is added to the pandas dataframe self.ninox_data.
        If not, the column is skipped.
        """
        genetics_file = self.ninox_filedict[self.survey_type]["Genetics"]
        # print to console that genetics file is being read and handled:
        print(f"\nreading in Genetics file {genetics_file} ...")

        ninox_genetics = pd.read_csv(os.getcwd() + f"/ninox/{genetics_file}")
        # rename columns in ninox_genetics to match needed_columns:
        ninox_genetics.rename(columns={"Projects":"Project", "Sample Name": "Sample.Name", "Genetic Latitude Pin": "Latitude", "Genetic Longitude Pin": "Longitude",
                                    "Genetics Survey Type": "Survey.Type", "Survey Date": "Survey.Date", "Genetic ID":"Genetic.ID"}, inplace=True)
        
        if self.survey_type == "Dog":
            ninox_genetics.rename(columns={"Scat ID":"Scat.ID"}, inplace=True)
        
        # extract the newest sample date from the ninox_genetics file:
        self.genetics_currency = ninox_genetics["Survey.Date"].dropna().max()

        # extract column names from ninox_genetics
        ninox_genetics_columns = ninox_genetics.columns
        #print("ninox_genetics_columns: ", ninox_genetics_columns)

        # iterate through ninox_genetics_columns and compare to needed_columns
        appended_columns = []
        for column in ninox_genetics_columns:
            if self.survey_type == "Dog":   
                # append extra columsn to needed columns
                if column in self.needed_columns or column in self.extra_columns:
                    appended_columns.append(column)
                else:
                    pass
            else: # all other survey types 
                if column in self.needed_columns:
                    appended_columns.append(column)
                else:
                    pass
        
        ninox_genetics["Survey.Type"] = self.survey_type

        # add columns that are in needed_columns but not in appended_columns to self.remaining_columns
        self.remaining_columns = [column for column in self.needed_columns if column not in appended_columns]
        # Remove "Survey.Type" from self.remaining_columns:
        self.remaining_columns.remove("Survey.Type")

        ninox_genetics["Survey.Date.Copy"] = ninox_genetics["Survey.Date"]
        ninox_genetics["Survey.Date"] = pd.to_datetime(ninox_genetics["Survey.Date"], format="%d/%m/%Y", errors='coerce').dt.date

        ### see if ninox data for this survey type has already been merged before, if so only extarct data newer than self.currency:
        ### if currency is not nan extract all samples newer than currency and append them to self.ninox_data_survey_type file.
        ### if newer data has been added, set ninox_remerge_survey to true, to remerge all ninox data for all surveys and overwrite the ninox_merged.csv file.
        if self.currency is not np.nan:
            # test for samples in genetics file newer than currency:
            self.genetics_currency = ninox_genetics["Survey.Date"].dropna().max()

            # write newest sample date to log file:
            logging.info(f"newest sample date in {genetics_file}: {self.genetics_currency}")

            if self.genetics_currency > self.currency:
                print("new samples found in ", genetics_file)
                # extract all samples newer than currency:
                self.ninox_genetics = ninox_genetics[ninox_genetics["Survey.Date"] > self.currency]
                # print shape of ninox_genetics_newer to log file:
                logging.info(f"shape of new samples df: {self.ninox_genetics.shape}")

            else:
                print(f"no new samples found in {genetics_file}")
                self.ninox_genetics = ninox_genetics
                # print shape of complete ninox_genetics to log file:
                logging.info(f"shape of complete samples df: {self.ninox_genetics.shape}")
        else:
            # survey type hasn't been handled before, use complete ninox_genetics:
            self.ninox_genetics = ninox_genetics
            # print shape of complete ninox_genetics to log file:
            logging.info(f"shape of complete samples df (initial handling): {self.ninox_genetics.shape}")
        return
    
    def read_Extractions(self):
        """This function reads in the "Extractions.csv" file and reads in the remaining columns needed which are stored
        in self.skipped_columns. The format of the Date extracted column is DD/MM/YYYY.
        In case of multiple extractions of a single sample there are duplicates of Sample.Name. """
        extractions_file = self.ninox_filedict[self.survey_type]["Extractions"]
        # print to console that extractions file is being read and handled:
        print(f"\nreading in Extractions file {extractions_file} ...")

        ninox_extractions = pd.read_csv(os.getcwd() + f"/ninox/{extractions_file}")
        ninox_extractions.columns = [col.strip() for col in ninox_extractions.columns] # remove leading and trailing whitespaces from column names.
        # print("self.skipped_columns: ", self.remaining_columns)   # remaining columns to be added from Extractions to ninox_data.

        # rename columns in ninox_extractions to match needed_columns:
        ninox_extractions.rename(columns={"Sample Name":"Sample.Name", "Protocol":"Extraction.Method", "Date extracted": "Date.Extraction", 
                                          "DART Sample ID (Sample name returned by DArT)": "Dart.Sample.ID", "Extraction ID":"Extraction.ID", "Genetic ID":"Genetic.ID", 
                                          "DART Order Number":"Dart.Order.Number"}, inplace=True)
        
        # print column names for ninox_extractions:
        # print("ninox_extractions columns: ", ninox_extractions.columns)

        # add "Sample.Name" to self.remaining_columns:
        self.remaining_columns.append("Sample.Name")

        # extract all samples newer than currency:
        # Convert the "Date.Extraction" column to datetime. In the extraction file the date format seems to be MM/DD/YYYY:
        ### APPARENTLY CURRENTLY NINOX DATE FORMAT IS DETERMINED BY BROWSER SETTINGS....

        # Convert the "Date.Extraction" column to datetime
        ninox_extractions["Date.Extraction"] = pd.to_datetime(ninox_extractions["Date.Extraction"], format="%m/%d/%Y", errors='coerce').dt.date

        # extract the newest sample date from the ninox_extractions file:
        self.extractions_currency = ninox_extractions["Date.Extraction"].dropna().max()

        # write newest sample date to log file:
        logging.info(f"newest sample date in {extractions_file}: {self.extractions_currency}")
        # print newest sample date in ninox_extractions to console:
        print("newest sample date in ninox_extractions: ", self.extractions_currency)
        # and print self currency:
        print("self.currency: ", self.currency)

        if pd.notnull(self.currency) and pd.notnull(self.extractions_currency): 
            if self.extractions_currency > self.currency:
                # extract all samples newer than currency:
                self.ninox_extractions = ninox_extractions[ninox_extractions["Date.Extraction"] > self.currency]
                # append shape to log.
                logging.info(f"shape of new samples df: {self.ninox_extractions.shape}")
                self.ninox_remerge_survey = True
                # print set self.ninox_remerge_survey to True, as new data was added. to log file:
                logging.info(f"set self.ninox_remerge_survey for survey_type {self.survey_type} to True, as new data was added.")
            else:
                self.ninox_extractions = ninox_extractions
                # print no newer samples found in {extractions_file}, hence no new data added to ninox data merged for survey type: {self.survey_type}.  to log file:
                logging.info(f"no newer samples found in {extractions_file}, hence no new data added to ninox data merged for survey type: {self.survey_type}.")
                # set self.ninox_remerge_survey to False, as no new data was added:
                self.ninox_remerge_survey = False
                # print set self.ninox_remerge_survey to False, as no new data was added. to log file:
                logging.info(f"set self.ninox_remerge_survey for survey_type {self.survey_type} to False, as no new data was added.")
        else:
            # survey type hasn't been handled before, use complete ninox_extractions:
            self.ninox_extractions = ninox_extractions
            # print shape of complete ninox_extractions to log file:
            logging.info(f"shape of complete samples df (initial handling): {self.ninox_extractions.shape}")
            self.ninox_remerge_survey = True
            # print set self.ninox_remerge_survey to True, as all data is new, to log file:
            logging.info(f"set self.ninox_remerge_survey for survey_type {self.survey_type} to True, as survey type hasn't been handled before.")

        return self.ninox_remerge_survey
    
    def merge_ninox_data_survey(self):
        """merge the genetics and extractions ninox data for each survey type individually, it will only be called if self.ninox_remerge_survey is True.
        Create a new ninox_merged_survey_type.csv file if it doesn't exist yet, otherwise append to it based on currency.
        If currency is nan, then it's the first time the ninox data is being merged for this survey type, so create a new file.
        If currency is not nan, then it's not the first time the ninox data is being merged for this survey type, so append to the existing file.
        If the ninox_remerge_survey bool is True, then overwrite the existing file.
        ninox_remerge_survey is set to True if new data was added to the ninox data for this survey type, 
        it is false if no new data was added or if extractions doesn't have new data even of genetics does.
        """
        print("\nmerging ninox genetics and extractions for survey type: ", self.survey_type)

        ##### Merge ninox data for each survey type individually:
        # Create directory if it doesn't exist
        os.makedirs("ninox_merged", exist_ok=True)

        # File path: check if ninox merged for survey type exists, if it does check if self.ninox_remerge_survey is True, 
        # if it is overwrite the file, if not exit and note so in log.:
        file_path = os.path.join(os.getcwd(), "ninox_merged", f"ninox_merged_{self.survey_type}.csv")

        # use pandas merge or join function to add data for the remaining columns from Extractions to ninox_genetics_data using the Sample.Name as index
        # first convert the dataframe columns "Sample.Name" to upper case:
        self.ninox_extractions["Sample.Name"] = self.ninox_extractions["Sample.Name"].str.upper()
        self.ninox_genetics["Sample.Name"] = self.ninox_genetics["Sample.Name"].str.upper()

        # sort Sample.Name using the ASCII table in ninox_data and ninox_extractions:
        self.ninox_extractions.sort_values(by="Sample.Name", inplace=True)
        self.ninox_genetics.sort_values(by="Sample.Name", inplace=True)

        # merge ninox_genetics and ninox_extractions:
        ninox_data = self.ninox_genetics.merge(self.ninox_extractions, how="inner", on="Sample.Name", suffixes=("_g", "_e"))

        ###########################################
        ### CLEAN UP DATA:
        # drop all columns in ninox_data which are not in self.needed_columns:
        keep_columns = [column for column in ninox_data.columns if column in self.needed_columns]
        # append all column names with '_g' and '_e' to keep_columns:
        keep_columns.extend([column for column in ninox_data.columns if column.endswith("_g") or column.endswith("_e")])
        # drop columns which contain no data unless they are in keep_columns:
        ninox_data.drop([column for column in ninox_data.columns if column not in keep_columns], axis=1, inplace=True)

        # check for GPS location in wrong format. Projects are likely to be located in Australia. 
        # Positive coordinates may result from Ninox data export without setting "use field settings" during export. Check for positive coordinates and convert them to negative in Latitude.
        # check if Latitude is positive, if so convert to negative:
        # coutn samples with positive Latitude and extract their rows into a new dataframe:
        positive_latitude = ninox_data[ninox_data["Latitude"] > 0]
        # print number of samples with positive Latitude to console and log:
        print(f"number of samples with positive Latitude: {positive_latitude.shape[0]}")
        logging.info(f"number of samples with positive Latitude (may result from exporting data from ninox without selecting >>use field settings<<): {positive_latitude.shape[0]}")
        # if there are samples with positive Latitude, convert them to negative:
        ninox_data.loc[ninox_data["Latitude"] > 0, "Latitude"] = ninox_data["Latitude"] * -1

        unique_dart_numbers = ninox_data["Dart.Order.Number"].unique()
        # if there is elements in the list other than nan, then replace "ko" with "Ko" in place:
        if len(unique_dart_numbers) > 1:
            ninox_data["Dart.Order.Number"].replace({"ko":"Ko"}, inplace=True)

        # print all unique DArt.Order.Numbers to console and log:
        print(f"unique DArt.Order.Numbers for {self.survey_type}: ", ninox_data["Dart.Order.Number"].unique())
        logging.info(f"unique DArt.Order.Numbers: {ninox_data['Dart.Order.Number'].unique()}")
        ###########################################
        
        ## if the file ninox_merged_survey_type.csv already exists, and currency is not nan, then append to the existing file:
        if os.path.isfile(file_path) and self.currency is not np.nan:
            print(f"ninox_merged_{self.survey_type}.csv already exists, new samples will be appended to {file_path}.")
            # read in the ninox_merged_survey_type.csv file:
            ninox_merged_survey_type = pd.read_csv(file_path)
            # add all samples newer than currency (self.ninox_data) to ninox_merged_survey_type:
            ninox_merged_survey_type = pd.concat([ninox_merged_survey_type, ninox_data], ignore_index=True)
            # count all duplicates from ninox_merged_survey_type:
            print("ninox_merged_survey_type duplicates: ", ninox_merged_survey_type.duplicated().sum())
            # write number of duplicates after appending new samples to log file:
            logging.info(f"number of duplicates after appending new samples: {ninox_merged_survey_type.duplicated().sum()}")
            # save ninox_merged_survey_type to file_path:
            ninox_merged_survey_type.to_csv(file_path, index=False)

        else:
            # ninox_merged_survey_type.csv doesn't exist yet, it will be created.
            print(f"ninox_merged_{self.survey_type}.csv does not exist yet, it will be created.") 
            # SAVE self.ninox_data to file_path:
            ninox_data.to_csv(file_path, index=False)
            # write to log file that ninox_merged_survey_type.csv will be created:
            logging.info(f"ninox_merged_{self.survey_type}.csv does not exist yet, it will be created.")

        return

    
    
class dart():
    """class to handle all operations for the DArT data. 
    The DArT data is contained in seperate folders for each DArT order following the naming convention DKoXX-XXXX, with X being numbers.
    Each folder should contain at least one report file which follows the naming convention Report_DKoXX-XXXX_*SNP*.csv. Sometimes multiple files match this convention, 
    the one with the shortest name is the being used to exclude extra suffixes.
    The report file contains many rows and columns, the Sample names are in row 7 starting in the column after "RepAvg". The col number differs depending on the DArT order.
    From order DKo21-6340 onwards there is also a SampleFile_DKoXX-XXXX.csv file which contains info to be added to the ninox data.
    The Sample File contains the following columns:
    "PlateID","Row","Column","Organism","Species","Genotype","Tissue","Comments".
    For older orders that file is called "DArT_extract*.csv, it contains the same column names.
    Orders from DKo18-3951 onwards contain this file, all older orders do not have any of these two files.
    """
    def __init__(self) -> None:
        self.dart_file_dict = {}
        self.l_all_dart_samples = []
        print("\nDArT data ...")
        # print to log file that now the DArT data will be handled:
        logging.info(f"now the DArT data will be handled.")
        pass

    def create_dart_file_dict(self):
        """
        create a dict which will contain the filenames for the individual DArT orders. 
        If there are multiple files matching the naming convention of the Report File all of them will be added, later the one with the shortest filename will be used.
        If the Sample File is not available, add NA instead.
        If no SampleFile but a DArT_extract file is available, add that instead."""
        dart_folder_list = glob.glob(os.getcwd() + "/DArT/" + "DKo[0-9]*", recursive=True)
        # iterate through dart_folder_list and create pandas dataframe.
        dart_datafiles = pd.DataFrame(columns=["dart_folder", "report_files", "sample_file"])
        for folder in dart_folder_list:
            folder_name = folder.rsplit(os.sep, 1)[-1] # equals dart order number

            # test if at least one report file is available:
            report_filenames = glob.glob(os.path.join(folder, f"Report_{folder_name}*SNP*.csv"))
            # KEEP ONLY THE ACTUAL FILENAME, NOT THE WHOLE PATH:
            report_filenames = [os.path.basename(filename) for filename in report_filenames]
            if len(report_filenames) == 0:
                report_filenames = "no Report file available"

            # test if a SampleFile is available
            sample_filename = glob.glob(os.path.join(folder, f"SampleFile*{folder_name}*"))
            if len(sample_filename) > 0:
                # KEEP ONLY THE ACTUAL FILENAME, NOT THE WHOLE PATH:
                sample_filename = os.path.basename(sample_filename[0])
            elif len(sample_filename) == 0:
                # test if a DArT_extract file is available
                dartextract_filename = glob.glob(os.path.join(folder, f"DArT*extract*"))
                if len(dartextract_filename) > 0:
                    # KEEP ONLY THE ACTUAL FILENAME, NOT THE WHOLE PATH:
                    dartextract_filename = os.path.basename(dartextract_filename[0])
                    sample_filename = dartextract_filename
                elif len(dartextract_filename) == 0:
                    sample_filename = "no SampleFile available"
            else:
                sample_filename = "no SampleFile available"

            # append results as new row to dart_datafiles dataframe:
            dict_row = {"dart_folder": folder_name, "report_files": report_filenames, "sample_file": sample_filename}
            # Convert dict_row to a DataFrame and concatenate it with dart_datafiles
            dart_datafiles = pd.concat([dart_datafiles, pd.DataFrame([dict_row])], ignore_index=True)

        # iterate through dart_datafiles and create a dict with the dart order number as key and the filenames as values:
        for index, row in dart_datafiles.iterrows():
            # extract the dart order number from the folder name:
            dart_order_number = row["dart_folder"]
            # create a new entry in the dart_file_dict with the dart order number as key and the filenames as value:
            self.dart_file_dict[dart_order_number] = {"report_files": row["report_files"], "sample_file": row["sample_file"]}
        print("self.dart_file_dict: \n", json.dumps(self.dart_file_dict, indent=4))

        # add the dart_file_dict to the log file, print this nicely.
        logging.info(f"dart_file_dict: \n {json.dumps(self.dart_file_dict, indent=4)}")

        return
    
    def check_all_dart_data_csv(self):
        # Check if all_dart_data.csv is available
        if os.path.exists('all_dart_data.csv'):
            print("\n >>> latest dart order number: ", max(self.dart_file_dict.keys()))

            # Function to ask the user for their choice
            def ask_user():
                return input("Do you want to re-gather the data? (yes/no): ")

            # Start a thread to ask the user for their choice
            thread = threading.Thread(target=ask_user)
            thread.start()

            # Wait for 10 seconds or until the user answers
            thread.join(timeout=10)

            # If the thread is still alive, the user didn't answer within 10 seconds
            if thread.is_alive():
                print('User did not respond, re-gathering the data...')
                # define user_decision parameter:
                user_decision = "yes"
            else:
                user_input = thread.result
                if user_input.lower() == 'yes':
                    user_decision = "yes"
                else:
                    # print that all_dart_data.csv will be used, as DArT orders included are up to date
                    # print to console and log, then exit function
                    user_decision = "no"
                    print("all_dart_data.csv will be used.")
                    logging.info("all_dart_data.csv will be used.")

        else:
            # print that all_dart_data.csv will be created, as it doesn't exist yet
            # print to console and log, then exit function
            user_decision = "yes"
            print("all_dart_data.csv will be created.")
            logging.info("all_dart_data.csv will be created.")

        return user_decision

    def iterate_DArT_data(self, user_decision = "no"):
        """iterate through all folders in dart_data directory which follow the DArT order naming convention DKoXX-XXXX, with X being numbers. 
        Create a new pandas dataframe for each DArT order.
        Then the Report file will be read and all sample names extracted as well as converted to upper case to match the Ninox naming conventions.
        In this function we need to extract all DArT sample names and if DArT-extract or SampleFile are available also extract
        the Tissue column.
        In these metadata files the "Genotype" column contains the Sample Name, which will be used for matching.
        """
        
        if user_decision == "yes":    
            print('all_dart_data.csv is not available or should be overwritten, re-gathering the data...')
            all_dart_data = pd.DataFrame(columns=["sample_names", "dart_order_number", "tissue"])
            # iterate through DArT orders:
            for item in self.dart_file_dict:
                # create a new pandas dataframe for each DArT order:
                dart_data = pd.DataFrame(columns=["dart_order_number", "sample_names", "sample_file"])
                # extract the dart order number from the folder name:
                dart_order_number = item
                # extract the filenames from the dart_file_dict:
                report_filenames = self.dart_file_dict[item]["report_files"]

                if not report_filenames == "no Report file available":
                    # select the report file with the shorter filename if there are more than 1:
                    if len(report_filenames) > 1:
                        report_filename = min(report_filenames, key=len)
                    else:
                        report_filename = report_filenames[0]
                    print("\n\n >>> report_filename: ", report_filename)

                    # READ THE REPORT FILE:
                    # read in the first 10 rows of the report file, find the row number of where the first column entry says AlleleID.
                    # This is the row where the actual data starts, but it varies between DArT orders.
                    # Use that row number to read in the whole file again, but skip the first n rows.
                    report_file = pd.read_csv(os.getcwd() + "/DArT/" + dart_order_number + "/" + report_filename, nrows=10, header=None, low_memory=False)
                    # find the row number of the row where the first column entry says AlleleID or from DKo22-7008 onwards MarkerName:
                    row_number = report_file[report_file.iloc[:,0].isin(["AlleleID", "MarkerName"])].index[0]
                    # read in the whole file again, but skip the first n rows:
                    report_file = pd.read_csv(os.getcwd() + "/DArT/" + dart_order_number + "/" + report_filename, skiprows=row_number, header=None, low_memory=False)
                    # Get row 0 (indexing starts at 0, so we use index 0) as first 6 rows are skipped at reading in:
                    row_7 = report_file.iloc[0]
                    # Find the column where "RepAvg" or from DKo22-7008 onwards "RatioAvgCountRefAvgCountSnp" appears
                    repavg_column = row_7[row_7.isin(["RepAvg", "RatioAvgCountRefAvgCountSnp"])].index[0]
                    print("----- repavg_column: ", repavg_column)
                    # extract all values from row_7 after the index of "RepAvg", which corresponds to all sample names:
                    sample_names = row_7.iloc[repavg_column+1:]
                    # convert all sample names to upper case:
                    sample_names = sample_names.str.upper()
                    # print length of sample_names:
                    print(f"----- length of sample_names from Report for {dart_order_number}: ", len(sample_names))
                else:
                    print("no Report available for ", dart_order_number)
                    continue    # move on to the next DArT order if there is no Report file available.

                # READ THE SAMPLE FILE/DArT_extract FILE:
                sample_filename = self.dart_file_dict[item]["sample_file"]
                print("\n >>> sample_filename: ", sample_filename)
                if not sample_filename == "no SampleFile available":
                    sample_file = pd.read_csv(os.getcwd() + "/DArT/" + dart_order_number + "/" + sample_filename)
                    # convert all sample names to upper case:
                    sample_file["Genotype"] = sample_file["Genotype"].str.upper()
                    # print length of sample_file:
                    print(f"----- length of sample_file from SampleFile for {dart_order_number}: ", len(sample_file))

                    # The report sample df should contain all two columns: sample names and the DArT order number. 
                    # The sample file df should contain all three columns: sample names, DArT order number and Tissue.
                    # These shall then be used to match the sample names between each other. 
                    # now create a new dataframe for each list of sample names:
                    report_samples_df = pd.DataFrame(columns=["sample_names", "dart_order_number"])
                    report_samples_df["sample_names"] = sample_names
                    report_samples_df["dart_order_number"] = dart_order_number

                    sample_file_df = pd.DataFrame(columns=["sample_names", "tissue"])
                    sample_file_df["sample_names"] = sample_file["Genotype"]
                    # convert all sample names to upper case:
                    sample_file_df["sample_names"] = sample_file_df["sample_names"].str.upper()
                    sample_file_df["tissue"] = sample_file["Tissue"]
                    # match the sample names between the two dataframes and merge them:
                    dart_data = report_samples_df.merge(sample_file_df, how="inner", on="sample_names")
                    # print length of dart_data:
                    print(f"----- length of dart_data for {dart_order_number}: ", len(dart_data))

                    # append dart_data to all_dart_data:
                    all_dart_data = pd.concat([all_dart_data, dart_data], ignore_index=True)
                    # print length of all_dart_data:
                    print(f"new length of all_dart_data for {dart_order_number}: ", len(all_dart_data))
                else:
                    # print to log that no SampleFile is available for this DArT order, hence only Report samples will be used to match DArT order numbers to Ninox sample names.
                    logging.info(f"no SampleFile available for {dart_order_number}, hence only Report samples will be used to match DArT order numbers to Ninox sample names.")
                    print("----- no SampleFile available for ", dart_order_number)
                    # no metadata available, only Report samples will be used to match DArT order numbers to Ninox sample names.
                    report_samples_df = pd.DataFrame(columns=["sample_names", "dart_order_number"])
                    report_samples_df["sample_names"] = sample_names
                    report_samples_df["dart_order_number"] = dart_order_number

                    dart_data = report_samples_df
                    # append column tissue type with NA:
                    dart_data["tissue"] = "NA"
                    # append dart_data to all_dart_data:
                    all_dart_data = pd.concat([all_dart_data, dart_data], ignore_index=True)
                     # print length of all_dart_data:
                    print(f"new length of all_dart_data for {dart_order_number}: ", len(all_dart_data))
                    
            #extract all sample names from all_dart_data:
            self.l_all_dart_samples = all_dart_data["sample_names"].tolist()

            # save all_dart_data to csv file:
            # if the dart_merged directory is not yet available, create it:
            os.makedirs("dart_merged", exist_ok=True)
            all_dart_data.to_csv(os.getcwd() + "/dart_merged/all_dart_data.csv", index=False)
            
        elif user_decision == "no":
            # print to console and log that all_dart_data.csv will be used for merging with ninox, as DArT orders included are up to date.
            print("all_dart_data.csv will as DArT orders included are up to date.")
            logging.info("all_dart_data.csv will not be overwritten, as DArT orders included are up to date.")
            
        return 
    

class combine_dart_ninox():
    def __init__(self):
        self.combined_data = pd.DataFrame()
        return

    def initial_combination(self):
        """
        this function reads in the ninox_merged file and the all_dart_data file and combines them into a new file."""
        # read in the ninox_merged file:
        ninox_merged = pd.read_csv(os.getcwd() + "/ninox_merged/ninox_merged.csv")
        # read in the all_dart_data file:
        all_dart_data = pd.read_csv(os.getcwd() + "/dart_merged/all_dart_data.csv")

        # print all unique DArt.Order.Numbers from ninox_merged to console and log:
        print("unique DArt.Order.Numbers from ninox_merged: ", ninox_merged["Dart.Order.Number"].unique())
        # print all unique DArt.Order.Numbers from all_dart_data to console and log:
        print("unique DArt.Order.Numbers from all_dart_data: ", all_dart_data["dart_order_number"].unique())

        # Make sure to reduce sources of errors for matching sample names due to case sensitivity, leading and trailing spaces and data type:
        # Convert sample names to the same case
        ninox_merged['Sample.Name'] = ninox_merged['Sample.Name'].str.upper()
        all_dart_data['sample_names'] = all_dart_data['sample_names'].str.upper()

        # Remove leading and trailing spaces
        ninox_merged['Sample.Name'] = ninox_merged['Sample.Name'].str.strip()
        all_dart_data['sample_names'] = all_dart_data['sample_names'].str.strip()

        # Convert sample names to the same data type
        ninox_merged['Sample.Name'] = ninox_merged['Sample.Name'].astype(str)
        all_dart_data['sample_names'] = all_dart_data['sample_names'].astype(str)

        # merge the two dataframes:
        combined_data = ninox_merged.merge(all_dart_data, how="inner", left_on="Sample.Name", right_on="sample_names")
        self.combined_data = combined_data

        ### Add shape of combined data to console and log file:
        print(f"shape of combined ninox and dart data: {self.combined_data.shape}")
        logging.info(f"shape of combined ninox and dart data: {self.combined_data.shape}")

        # print unique DArt.Order.Numbers from combined_data to console and log:
        print("unique DArT order numbers from combined_data: ", self.combined_data["dart_order_number"].unique())


        return
    
    def append_combination(self):
        """
        this function is called if there has been a combination of ninox and dart data previously. 
        Only new data is added. The mastersheet is used to determine the last entry."""
        return
    
    def check_data_and_count_unmatched_samples(self):
        """
        this function checks the combined data for unmatched samples and saves them to a log file.
        Counts for how many DArT samples couldn't be matched to a ninox sample and vice versa.
        Counts for mismatches between dart order names for the same sample.
        TODO: clean up and rename columns, some are double as they were taken from multiple sources.
        then save combined data
        """
        # group the combined data by dart order and count the number of samples, use the DArT data as reference, as it is more complete and was matched to the exisitng ninox data:
        dart_groups = self.combined_data.groupby("dart_order_number").count()
        # print dart_groups:
        print("dart_groups: \n", dart_groups)
        # save dart_groups to a log file, print only the DAart order number and the number of samples for that order number:
        logging.info(f"dart_groups: \n {dart_groups['Sample.Name']}")


         # save combined_data to csv file:
        self.combined_data.to_csv(os.getcwd() + "/dart_merged/combined_ninox_and_dart_data.csv", index=False)
        return
    


######################################################
### MAIN SCRIPT ###
######################################################
skip_ninox = False   # set to True if the ninox data has already been downloaded and is up to date and only handle the DArT data.
if __name__ == "__main__":
    ### create a new log file calles logfile.log, or if it exists already append to it.
    ### print a start of script line with Date and Time to log file to see when the script was started.
    logging.basicConfig(filename='logfile.log', level=logging.INFO, format='%(asctime)s %(message)s')
    logging.info(f"\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\nStart of script: combine_dart_and_ninox_samples_2.py")
    # write skip_ninox to log file:
    logging.info(f"boolean skip_ninox: {skip_ninox}")

    # handle the ninox data:
    # get the data status of the ninox_merged data (all survey types combined), 
    # if it's true (ninox_merged.csv already exists) only newer samples than ninox_merged currency will be handled in survye_type,
    # else assemble the data from scratch, this functionality is implemented within the ninox_survey class.:
    ninox_all = ninox_all(ninox_filedict=ninox_filedict)
    ninox_data_status = ninox_all.determine_data_status()
    ninox_all_currency = ninox_all.test_currency()

    #print data status and currency to log file:
    logging.info(f"ninox_data_status: {ninox_data_status}")
    logging.info(f"ninox_all_currency: {ninox_all_currency}")
    # and to console:
    print(f"ninox_data_status: {ninox_data_status}")
    print(f"ninox_all_currency: {ninox_all_currency}")

    # initiate a ninox class for each survey type to combine Genetics and Extraction data of each individually:
    ninox_dog = ninox_survey(ninox_filedict=ninox_filedict, survey_type="Dog", ninox_all_currency=ninox_all_currency)
    ninox_drone = ninox_survey(ninox_filedict=ninox_filedict, survey_type="Drone", ninox_all_currency=ninox_all_currency)
    ninox_opportunistic = ninox_survey(ninox_filedict=ninox_filedict, survey_type="Opportunistic", ninox_all_currency=ninox_all_currency)
    ninox_tracking = ninox_survey(ninox_filedict=ninox_filedict, survey_type="Tracking", ninox_all_currency=ninox_all_currency)
    ninox_partner = ninox_survey(ninox_filedict=ninox_filedict, survey_type="Partner", ninox_all_currency=ninox_all_currency)

    # read in the Genetics and Extractions data for each survey type:
    ninox_dog.read_Genetics()
    ninox_remerge_survey_dog = ninox_dog.read_Extractions()
    ninox_drone.read_Genetics()
    ninox_remerge_survey_drone = ninox_drone.read_Extractions()
    ninox_opportunistic.read_Genetics()
    ninox_remerge_survey_opportunistic = ninox_opportunistic.read_Extractions()
    ninox_tracking.read_Genetics()
    ninox_remerge_survey_tracking = ninox_tracking.read_Extractions()
    ninox_partner.read_Genetics()
    ninox_remerge_survey_partner = ninox_partner.read_Extractions()

    # create a dict with all survey types, their class instances and the returned ninox_remerge_survey bools as {"survey_type": {"class": ninox_survey_type, "bool": ninox_remerge_survey}}:
    ninox_remerge_survey_dict = {"Dog": {"class": ninox_dog, "bool": ninox_remerge_survey_dog},
                                "Drone": {"class": ninox_drone, "bool": ninox_remerge_survey_drone},
                                "Opportunistic": {"class": ninox_opportunistic, "bool": ninox_remerge_survey_opportunistic},
                                "Tracking": {"class": ninox_tracking, "bool": ninox_remerge_survey_tracking},
                                "Partner": {"class": ninox_partner, "bool": ninox_remerge_survey_partner}
                                }
    # print only keys and bools, but not class from ninox_remerge_survey_dict to log file in a nice json format:
    logging.info(f"ninox_remerge_survey_dict: \n {json.dumps({key: value['bool'] for key, value in ninox_remerge_survey_dict.items()}, indent=4)}")

    # merge the ninox data for each survey type individually, if data for survey type doesn't exist yet, 
    # or if new data (newer than ninox_all_currency) was added for this survey type. ninox_remerge_survey is set to True in that case.
    # iterate through the ninox_remerge_survey_dict and merge the ninox data for each survey type individually:
    for survey_type in ninox_remerge_survey_dict:
        # extract the ninox_remerge_survey bool for each survey type:
        ninox_remerge_survey = ninox_remerge_survey_dict[survey_type]["bool"]
        # extract the ninox_survey class instance for each survey type:
        ninox_survey_type = ninox_remerge_survey_dict[survey_type]["class"]
        # print to log file that ninox_remerge_survey is True for survey type: {survey_type}:
        logging.info(f"ninox_remerge_survey is {ninox_remerge_survey} for survey type: {survey_type}, merged ninox data for this survey type will be saved.")
        if ninox_remerge_survey:
            # merge the ninox data for each survey type individually:
            ninox_survey_type.merge_ninox_data_survey()
        else:
            # print to log file that ninox_remerge_survey is False for survey type: {survey_type}:
            logging.info(f"ninox_remerge_survey is {ninox_remerge_survey} for survey type: {survey_type}, no new data was saved.")

    print("\n\n>> individual survey types of ninox data handling finished <<")
    print(">> now merging all survey types into one large ninox_merged file ... <<\n\n")
    # now merge all ninox data for all survey types into one file:
    # if data status is True (ninox_merged.csv already exists) only newer samples than ninox_merged currency will be appended for each survey type.
    ninox_all.merge_ninox_data_all()
            

    # now handle the DArT data:
    print("\n\n>> now handling DArT data ... <<\n\n")
    dart = dart()
    dart.create_dart_file_dict()
    user_decision = dart.check_all_dart_data_csv()  # check if all_dart_data.csv is available, if not ask user if they want to re-gather the data.
    dart.iterate_DArT_data(user_decision)

    # combine the ninox and DArT data:
    print("\n\n>> now combining ninox and DArT data ... <<\n\n")
    combine_dart_ninox = combine_dart_ninox()
    combine_dart_ninox.initial_combination()
    combine_dart_ninox.check_data_and_count_unmatched_samples()
     
