
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog
import yaml


import tkinter as tk
from tkinter import filedialog

def load_yaml_file(file_path):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data

def choose_file(messageStr,fileExt):
    # Create the Tkinter window
    root = tk.Tk()
    root.title(messageStr)

    # Create a label with the custom message
    label = tk.Label(root, text=messageStr)
    label.pack(pady=10)

    # Hide the Tkinter window
    root.iconify()
    messagebox.showinfo("Message", messageStr)

    # Open the file dialog
    file_path =  filedialog.askopenfilename(title= messageStr,filetypes=[("File Type", fileExt)])

    # Destroy the Tkinter window
    root.destroy()

    if file_path:
        return file_path
    else:
        quit()

def choose_folder(messageStr):
    # Create a Tkinter window
    root = tk.Tk()
    root.title(messageStr)

    # Create a label with the custom message
    label = tk.Label(root, text=messageStr)
    label.pack(pady=10)

    # Hide the Tkinter window
    root.iconify()

    # Open the folder dialog
    messagebox.showinfo("Message", messageStr)

    folder_path = filedialog.askdirectory()

    # Destroy the Tkinter window
    root.destroy()

    if folder_path:
        return folder_path
    else:
        quit()