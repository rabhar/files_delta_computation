import os
import pandas as pd
import difflib
import hashlib
import datetime
import shutil
import pypdf

class Delta:

    def __init__(self, report_file, update_type = "content", truncate = False) -> None:
        self.report_file = report_file
        self.new_files = []
        self.deleted_files = []
        self.updated_files = []
        self.update_type = update_type
        self.truncate = truncate
 
    def get_files_in_path(self, dir, lvl = 0):
        paths = []
        for file_obj in os.scandir(dir):
            if file_obj.is_dir():
                paths.extend(self.get_files_in_path(file_obj, lvl = lvl))
            else:
                paths.append(file_obj.path[lvl:] if type(dir) == str else file_obj.path[lvl:])
        
        return paths
            

    def get_files_dict(self, dir):

        files_dict = dict()
        for file in self.get_files_in_path(dir, len(dir)):
            files_dict[file] = datetime.datetime.fromtimestamp(os.path.getmtime(dir + file))

        return files_dict

    def create_files_dict(self):
        self.source_files_dict = self.get_files_dict(self.source_dir)
        self.target_files_dict = self.get_files_dict(self.target_dir)

    def get_delta_by_ts(self):
        source_files = set(self.source_files_dict.keys())
        target_files = set(self.target_files_dict.keys())

        self.new_files = source_files.difference(target_files)
        self.deleted_files = target_files.difference(source_files)

        present_in_both = source_files.intersection(target_files)
        self.updated_files = [[file, self.source_files_dict[file].strftime("%Y-%m-%d %H:%M:%S"), self.target_files_dict[file].strftime("%Y-%m-%d %H:%M:%S")]  for file in present_in_both if self.source_files_dict[file] > self.target_files_dict[file]]


    def hash_compare(self, file):
        return (
            hashlib.md5(open(self.source_dir + file, "rb").read()).hexdigest() != 
            hashlib.md5(open(self.target_dir + file, "rb").read()).hexdigest()
            )

    def get_bytes_from_pdf_page(self, pdf, page_num):
        contents = pdf.pages[page_num].get_contents()
        if isinstance(contents, pypdf.generic._data_structures.ArrayObject):
            return b"".join([content.get_object().get_data() for content in contents])
        else:
            return contents.get_data()


    def pdf_compare(self, file):
        
        source_pdf = pypdf.PdfReader(self.source_dir + file, "r")
        target_pdf = pypdf.PdfReader(self.target_dir + file, "r")
        diff_pages = [f"File: {file}"]
        
        for page_num in range(min(len(source_pdf.pages), len(target_pdf.pages))):
            if (hashlib.md5(self.get_bytes_from_pdf_page(source_pdf, page_num)).hexdigest() != 
                hashlib.md5(self.get_bytes_from_pdf_page(target_pdf, page_num)).hexdigest()):
                diff_pages.append(f"Page : {page_num + 1} content is different")

        return "\n".join(diff_pages)


    def text_compare(self, file):
        differ = difflib.Differ()
        diffs = differ.compare(open(self.source_dir + file, "r").readlines(), open(self.target_dir + file, "r").readlines())
        
        line_num = 0
        diff_lines = [f"File: {file}"]


        for line in diffs:
            symbol = line[:2]
        
            if symbol == "- ":
                diff_lines.append(f"line: {line_num} in source content, {line[2:].strip()}")

            
            elif symbol == "+ ":
                diff_lines.append(f"line: {line_num} in target content, {line[2:].strip()}")
            
            else:
                line_num += 1
        
        return "\n".join(diff_lines)
                

    def get_file_extension(self, file):
        return file.split(".")[-1]

    def get_delta_by_file_content(self):

        for idx, file in enumerate(self.updated_files):
            if self.get_file_extension(file[0]) in ("csv", "tsv", "txt"):
                self.updated_files[idx].append(self.text_compare(file[0]))
            elif self.get_file_extension(file[0]) == "pdf":
                self.updated_files[idx].append(self.pdf_compare(file[0]))
            else:
                self.updated_files[idx].append("Mismatch" if self.hash_compare(file[0]) else "" )
                
        
    def generate_report(self):
        new_sheet = pd.DataFrame(self.new_files, columns=["file_name"])
        del_sheet = pd.DataFrame(self.deleted_files, columns=["file_name"])
        update_sheet = pd.DataFrame(self.updated_files, columns=["file_name", "updated_timestamp_in_source_dir", "updated_timestamp_in_target_dir", "changed_content"])

        if self.update_type == "content":
            update_sheet = update_sheet[update_sheet["changed_content"] != ""]

        writer = pd.ExcelWriter(self.report_file, engine="xlsxwriter")
        update_sheet.to_excel(writer, sheet_name="updated_files", index=False)
        new_sheet.to_excel(writer, sheet_name="new_files", index=False)
        del_sheet.to_excel(writer, sheet_name="deleted_files", index=False)
        writer.close()



    def perform_sync(self):
        for file in self.new_files:
            if os.path.exists(self.target_dir + os.path.dirname(file)):
                shutil.copy(self.source_dir + file, self.target_dir + file)
            else:
                os.makedirs(self.target_dir + os.path.dirname(file))
                shutil.copy(self.source_dir + file, self.target_dir + file)
        
        for file in self.deleted_files:
            os.remove(self.target_dir + file)

        for file in self.updated_files:
            if (self.update_type == "ts") or (self.update_type == "content" and file[3]):
                shutil.copy(self.source_dir + file[0], self.target_dir + file[0])


    def compute_delta(self, source_dir, target_dir):
        self.source_dir = source_dir
        self.target_dir = target_dir

        self.create_files_dict()
        self.get_delta_by_ts()
        self.get_delta_by_file_content()
        self.generate_report()
        self.perform_sync()


Delta("delta_report.xlsx", update_type="content").compute_delta("source", "target")


