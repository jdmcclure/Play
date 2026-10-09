import csv
import os
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, ttk

from assigner import assign_measures, clean_list, group_by_person

#Colors used throughout the window
BACKGROUND = "#F3F5F4"
CARD = "#FFFFFF"
PRIMARY = "#1E4D2B"
PRIMARY_HOVER = "#2B6A3C"
TEXT = "#1F2933"
MUTED = "#6B7780"
BORDER = "#D5DBD6"
STRIPE = "#EEF4EF"
DISABLED = "#A9B4AD"

FONT = "Helvetica"


class BallotAssignerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Ballot Measure Assigner")
        self.geometry("1100x700")
        self.minsize(860, 560)
        self.configure(bg=BACKGROUND)

        self.assignments = []
        self.assigned_names = []
        self.view = tk.StringVar(value="measure")
        self.status = tk.StringVar(value="Enter ballot measures and names, then click Assign.")

        self._build_styles()
        self._build_header()
        self._build_body()
        self._build_footer()

        self.bind("<Command-Return>", lambda event: self.assign())
        self.bind("<Control-Return>", lambda event: self.assign())

    # ---------- Layout ----------

    def _build_styles(self):
        style = ttk.Style(self)
        style.theme_use("clam")

        style.configure("TFrame", background=BACKGROUND)
        style.configure("Card.TFrame", background=CARD, bordercolor=BORDER, relief="solid", borderwidth=1)
        style.configure("TLabel", background=BACKGROUND, foreground=TEXT, font=(FONT, 13))
        style.configure("Title.TLabel", font=(FONT, 24, "bold"), foreground=PRIMARY)
        style.configure("Subtitle.TLabel", foreground=MUTED)
        style.configure("CardTitle.TLabel", background=CARD, font=(FONT, 15, "bold"))
        style.configure("CardHint.TLabel", background=CARD, foreground=MUTED, font=(FONT, 12))
        style.configure("Status.TLabel", foreground=MUTED, font=(FONT, 12))

        style.configure("TButton", font=(FONT, 13), padding=(14, 7), background=CARD, foreground=TEXT,
                        bordercolor=BORDER, focuscolor=CARD)
        style.map("TButton", background=[("disabled", BACKGROUND), ("active", STRIPE)],
                  foreground=[("disabled", DISABLED)])
        style.configure("Primary.TButton", font=(FONT, 14, "bold"), padding=(22, 9), background=PRIMARY,
                        foreground="#FFFFFF", bordercolor=PRIMARY, focuscolor=PRIMARY)
        style.map("Primary.TButton", background=[("disabled", DISABLED), ("active", PRIMARY_HOVER)],
                  foreground=[("disabled", "#FFFFFF")])
        style.configure("Link.TButton", font=(FONT, 12), padding=(8, 3))

        style.configure("TRadiobutton", background=CARD, foreground=TEXT, font=(FONT, 12), focuscolor=CARD)
        style.map("TRadiobutton", background=[("active", CARD)])

        style.configure("Treeview", background=CARD, fieldbackground=CARD, foreground=TEXT, rowheight=30,
                        font=(FONT, 13), borderwidth=0)
        style.map("Treeview", background=[("selected", PRIMARY_HOVER)], foreground=[("selected", "#FFFFFF")])
        style.configure("Treeview.Heading", background=PRIMARY, foreground="#FFFFFF", font=(FONT, 13, "bold"),
                        padding=(8, 6), relief="flat")
        style.map("Treeview.Heading", background=[("active", PRIMARY_HOVER)])

    def _build_header(self):
        header = ttk.Frame(self, padding=(24, 20, 24, 8))
        header.pack(fill="x")
        ttk.Label(header, text="Ballot Measure Assigner", style="Title.TLabel").pack(anchor="w")
        ttk.Label(header, text="Randomly hand out ballot measures so that each one goes to exactly one person.",
                  style="Subtitle.TLabel").pack(anchor="w", pady=(2, 0))

    def _build_body(self):
        body = ttk.Frame(self, padding=(24, 8, 24, 8))
        body.pack(fill="both", expand=True)
        body.columnconfigure(0, weight=2, uniform="body")
        body.columnconfigure(1, weight=3, uniform="body")
        body.rowconfigure(0, weight=1)
        body.rowconfigure(1, weight=1)

        self.measures_text, self.measures_count = self._build_input_card(body, "Ballot Measures", row=0)
        self.names_text, self.names_count = self._build_input_card(body, "Names", row=1)
        self._build_results_card(body)

    def _build_input_card(self, parent, title, row):
        card = ttk.Frame(parent, style="Card.TFrame", padding=14)
        card.grid(row=row, column=0, sticky="nsew", padx=(0, 10), pady=(0 if row == 0 else 10, 0))
        card.columnconfigure(0, weight=1)
        card.rowconfigure(1, weight=1)

        top = ttk.Frame(card, style="Card.TFrame", borderwidth=0)
        top.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        ttk.Label(top, text=title, style="CardTitle.TLabel").pack(side="left")
        count = ttk.Label(top, text="0 entered", style="CardHint.TLabel")
        count.pack(side="left", padx=(10, 0))

        text = tk.Text(card, height=6, wrap="word", font=(FONT, 13), bg=CARD, fg=TEXT, insertbackground=TEXT,
                       relief="flat", highlightthickness=1, highlightbackground=BORDER, highlightcolor=PRIMARY,
                       padx=8, pady=6, undo=True)
        text.grid(row=1, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(card, orient="vertical", command=text.yview)
        scrollbar.grid(row=1, column=1, sticky="ns")
        text.configure(yscrollcommand=scrollbar.set)

        ttk.Button(top, text="Load from file…", style="Link.TButton",
                   command=lambda: self.load_file(text)).pack(side="right")
        ttk.Label(card, text="One per line. Duplicates and blank lines are ignored.",
                  style="CardHint.TLabel").grid(row=2, column=0, columnspan=2, sticky="w", pady=(6, 0))

        text.bind("<KeyRelease>", lambda event: self._update_count(text, count))
        text.bind("<<Paste>>", lambda event: self.after(10, lambda: self._update_count(text, count)))
        text.count_label = count
        return text, count

    def _build_results_card(self, parent):
        card = ttk.Frame(parent, style="Card.TFrame", padding=14)
        card.grid(row=0, column=1, rowspan=2, sticky="nsew", padx=(10, 0))
        card.columnconfigure(0, weight=1)
        card.rowconfigure(1, weight=1)

        top = ttk.Frame(card, style="Card.TFrame", borderwidth=0)
        top.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        ttk.Label(top, text="Assignments", style="CardTitle.TLabel").pack(side="left")
        ttk.Radiobutton(top, text="By person", value="person", variable=self.view,
                        command=self.show_results).pack(side="right")
        ttk.Radiobutton(top, text="By measure", value="measure", variable=self.view,
                        command=self.show_results).pack(side="right", padx=(0, 10))

        self.tree = ttk.Treeview(card, columns=("first", "second"), show="headings", selectmode="browse")
        self.tree.grid(row=1, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(card, orient="vertical", command=self.tree.yview)
        scrollbar.grid(row=1, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.tag_configure("stripe", background=STRIPE)
        self.tree.tag_configure("empty", foreground=MUTED)
        self.show_results()

    def _build_footer(self):
        footer = ttk.Frame(self, padding=(24, 8, 24, 20))
        footer.pack(fill="x")
        self.assign_button = ttk.Button(footer, text="Assign", style="Primary.TButton", command=self.assign)
        self.assign_button.pack(side="left")
        self.export_button = ttk.Button(footer, text="Export to Excel…", command=self.export, state="disabled")
        self.export_button.pack(side="left", padx=(10, 0))
        ttk.Button(footer, text="Clear All", command=self.clear).pack(side="left", padx=(10, 0))
        ttk.Label(footer, textvariable=self.status, style="Status.TLabel").pack(side="right")

    # ---------- Helpers ----------

    def _lines(self, text):
        return text.get("1.0", "end").splitlines()

    def _update_count(self, text, label):
        label.configure(text=f"{len(clean_list(self._lines(text)))} entered")

    # ---------- Actions ----------

    def load_file(self, text):
        file_path = filedialog.askopenfilename(
            title="Choose a list",
            filetypes=[("Text or CSV files", "*.txt *.csv"), ("All files", "*.*")],
        )
        if not file_path:
            return
        try:
            with open(file_path, newline="", encoding="utf-8-sig") as file:
                if file_path.lower().endswith(".csv"):
                    #Use the first column of each row
                    items = [row[0] for row in csv.reader(file) if row]
                else:
                    items = file.read().splitlines()
        except (OSError, UnicodeDecodeError) as error:
            messagebox.showerror("Could not load file", str(error))
            return

        text.delete("1.0", "end")
        text.insert("1.0", "\n".join(clean_list(items)))
        self._update_count(text, text.count_label)

    def assign(self):
        raw_measures = [line for line in self._lines(self.measures_text) if line.strip()]
        raw_names = [line for line in self._lines(self.names_text) if line.strip()]
        measures = clean_list(raw_measures)
        names = clean_list(raw_names)

        try:
            self.assignments = assign_measures(measures, names)
        except ValueError as error:
            messagebox.showwarning("Missing information", str(error))
            return

        self.assigned_names = names
        self.show_results()
        self.assign_button.configure(text="Reshuffle")
        self.export_button.configure(state="normal")

        people = "person" if len(names) == 1 else "people"
        message = f"{len(measures)} measures assigned across {len(names)} {people}."
        duplicates = (len(raw_measures) - len(measures)) + (len(raw_names) - len(names))
        if duplicates:
            message += f" {duplicates} duplicate(s) ignored."
        if len(names) > len(measures):
            message += f" {len(names) - len(measures)} name(s) received no measure."
        self.status.set(message)

    def show_results(self):
        self.tree.delete(*self.tree.get_children())

        if self.view.get() == "measure":
            headings = ("Ballot Measure", "Assigned To")
            rows = [(measure, name, False) for measure, name in self.assignments]
        else:
            headings = ("Name", "Ballot Measure")
            rows = []
            for name, measures in group_by_person(self.assignments, self.assigned_names).items():
                if not measures:
                    rows.append((name, "— none —", True))
                #Only show the name on the first row for each person
                for i, measure in enumerate(measures):
                    rows.append((f"{name}  ({len(measures)})" if i == 0 else "", measure, False))

        self.tree.heading("first", text=headings[0], anchor="w")
        self.tree.heading("second", text=headings[1], anchor="w")
        self.tree.column("first", anchor="w", width=300, stretch=True)
        self.tree.column("second", anchor="w", width=300, stretch=True)

        for i, (first, second, is_empty) in enumerate(rows):
            tags = [tag for tag, wanted in (("stripe", i % 2 == 1), ("empty", is_empty)) if wanted]
            self.tree.insert("", "end", values=(first, second), tags=tags)

    def export(self):
        if not self.assignments:
            return
        try:
            from excel_export import export_to_excel
        except ImportError:
            messagebox.showerror("Excel export unavailable",
                                 "The openpyxl package is required for Excel export.\n\n"
                                 "Install it with:  pip install openpyxl")
            return

        file_path = filedialog.asksaveasfilename(
            title="Export assignments",
            defaultextension=".xlsx",
            filetypes=[("Excel workbook", "*.xlsx")],
            initialfile=f"ballot_assignments_{datetime.now():%Y-%m-%d}.xlsx",
        )
        if not file_path:
            return
        try:
            export_to_excel(self.assignments, self.assigned_names, file_path)
        except OSError as error:
            messagebox.showerror("Could not save file", f"{error}\n\nIf the file is open in Excel, close it and try again.")
            return
        self.status.set(f"Exported to {os.path.basename(file_path)}")

    def clear(self):
        for text in (self.measures_text, self.names_text):
            text.delete("1.0", "end")
            self._update_count(text, text.count_label)
        self.assignments = []
        self.assigned_names = []
        self.show_results()
        self.assign_button.configure(text="Assign")
        self.export_button.configure(state="disabled")
        self.status.set("Enter ballot measures and names, then click Assign.")


if __name__ == "__main__":
    BallotAssignerApp().mainloop()
