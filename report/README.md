University Name: Birmingham City University
Student Name: Nabin Oli
Student ID: 23189629
Degree: BSc hons Computer and Data Science

```
File	
Approx. prose words
content/01-candidate-datasets.tex
208 (target ~200)
content/02-pipeline-design.tex
324 (target ~330)
content/03-storage-plan.tex
325 (target ~320)
content/04-reflection.tex
134 (target ~150)
Total
991

```






CMP6230 Coursework Task Sheet 1
Data Analytics Pipeline: Planning and Design
Your first task will be to plan and design a data management and data analytics strategy based on
using data within a specific problem domain, to produce analytics insights and machine learning models.
This will be documented within your log report.
The purpose of this task is to contribute towards meeting the module learning outcome: “Model and
effectively communicate data management requirements for analytical operations”
Underlined are the four key parts of the learning outcome, but what do they mean?
• Model1
o You will be expected to provide detailed evidence of the relevant structures and
processes used to meet the aim (producing a pipeline capable of analytical operations)
• Effectively communicate
o This evidence produced from the modelling process should be clearly explained and
well justified throughout, to allow the reader to better understand your thought process
when attempting the planning and design task.
• Data Management requirements
o The modelling process needs to consider not only how the data will be stored, but how it
will be used, this should also be communicated appropriately.
• Analytical Operations
o The modelling process must also consider that the stored data will be used to develop
Machine Learning models and apply various analytics techniques. These considerations
must also be communicated to the reader.
Note
The information contained within this document may be subject to further change to provide further
clarification of aspects of the coursework, or to provide additional advice/guidance for the task.
1 Remember, we can think of models as abstract representations of a given object or phenomena of interest with a focus on
emphasising relevant information and removing irrelevant information. They are used for the purpose of reviewing and
analysing that model to better understand the object or phenomena it represents and can also be used for predicting certain
behaviours or events of that object or phenomena based on the properties, structure and constraints that the model captures.
CMP6230 Coursework Task 1 (2024/2025)
S Iranmanesh 10/2024 – Page 2 of 7
Getting started
Exercise 1: Step 1
Identify 3 candidate source data sets2 that could be leveraged to develop a Machine Learning model to
solve a supervised learning task (e.g. a supervised learning model to determine whether a given flight is
likely to be delayed based on past data).
For each candidate source data set you should attempt to discuss and justify the following:
• What is it?
o e.g., what data does it represent?
• Who / what was it collected from?
o Where was the data originally sampled from?
▪ e.g. Is it collected from particular patients at a particular set of hospitals?
• Why was it collected?
o e.g., why does this data exist in the first place?
• Where did you get it from? o Where did you find it?
• What is its logical structure? o e.g., Is this source data a “flat file”?, “a collection of tables from a
relational database?”, etc.
o What does it “look” like structurally? Document this
▪ Show the logical structure of the input data using an appropriate diagram such as
an ERD
• How is it currently stored?
o e.g., Is it from a local database? A file? A web service?
o e.g., What kind of database? How is the data currently physically stored using that
database engine?
o e.g., What kind of file? How is data physically stored within that file on the file system?
• How could it be used to develop a machine learning model?
o What parts of the dataset could be leveraged to develop the model?
▪ e.g in a supervised learning task: what is the target variable and what are the
predictor variables?
▪ What is the “Ground truth”?
o Has this dataset previously been used to develop Machine Learning models?
▪ If not, why?
2 We recommend at least one of them be the same dataset you and your research team are leveraging in CMP6202
CMP6230 Coursework Task 1 (2024/2025)
S Iranmanesh 10/2024 – Page 3 of 7
Choose one of the candidate datasets that you believe would be suitable for developing Machine
Learning, this will be used for the later planning of a data analytics pipeline intended to ingest and utilise
the data within this dataset. Discuss the reasoning behind your choice of data set, making sure you
precisely identify how you believe the source data set could be leveraged for Machine Learning.
Warning!
For the particularly ambitious, you may wish to investigate how two or more dataset sources could be
combined from their different data sources into a single dataset3 to augment your dataset with
additional data from another source.
A note of caution: you must be very careful when attempting to establish relationships between data
from disparate sources as they may not necessarily be suitable to combine.
3 We sometimes call this “Data Blending” when it is done for a specific use case, this can also be compared with the ETL
process
CMP6230 Coursework Task 1 (2024/2025)
S Iranmanesh 10/2024 – Page 4 of 7
What you need to do
Exercise 1: Step 2
Plan out, justify and iteratively refine the design of an appropriate data analytics pipeline for the storage
and processing of a given data set, the data analytics pipeline should meet the needs of two major
consumers of the data:
• Reporting and Ad hoc querying by Data Scientists, Data Engineers, Business Analysts
o These consumers of the data should be able to run ad-hoc queries and generate reports on
the ingested data stored in your analytics data store / data base by accessing the database in
an analytics environment such as an IPython notebook.
• The Automated Machine Learning pipeline
o This consumer of the data will retrieve the data from the analytics store for the purpose of
building supervised machine learning models in an automated fashion
This section of the log report should include a detailed discussion of the following stages of the data
analytics pipeline:
• Data Ingestion
• Data Pre-processing
• Model Development
• Model Deployment
• Model Monitoring
For each stage, you should aim to discuss
• The What
o What is the stage being undertaken?
▪ e.g. what does “data ingestion” actually mean?
o What are the key components, systems and concepts that will make up this stage?
▪ e.g what tools and techniques are involved in the data ingestion process?
o What are the input dependencies to this stage?
▪ e.g. the data ingestion stage will likely require data
• How will this data be passed into this stage of the pipeline?
• Are there any requirements as to how that data needs to be represented/structured as input? If so,
why?
o What are the expected outputs of this stage?
▪ e.g. once the data ingestion stage is completed, what will the later stages actually
“get” out of this stage?
• The Why
o Why is this stage important?
▪ e.g why is the data pre-processing stage needed?
o Why are the key components, systems and concepts that make up this stage important?
▪ e.g what does each tool or technique contribute to the task being undertaken?
• The Who
o Who is involved with completing this stage?
▪ Within an organisation, which people/roles would need to be involved in taking the
input dependencies and processing them into an appropriate form to create the
outputs to this stage?
CMP6230 Coursework Task 1 (2024/2025)
S Iranmanesh 10/2024 – Page 5 of 7
o Who benefits from the outputs of this stage?
▪ Within an organisation, the outputs of each stage of the pipeline must provide
benefit to someone, which people/roles would likely benefit from this stage?
• The How
o How will the stage be completed?
▪ Each stage can be thought of as transforming its input dependencies into outputs to
pass on to the later stages of the pipeline
▪ But each stage itself has its own “internal pipeline” where we need to compose one
or more internal processes to transform our inputs into our outputs
▪ What are these individual processes that form the “internal pipeline” we need to
compose?
▪ What tools and techniques could be used to execute each process?
CMP6230 Coursework Task 1 (2024/2025)
S Iranmanesh 10/2024 – Page 6 of 7
Exercise 1: Step 3
The documentation of your pipeline should be accompanied by an appropriate plan for the storage of data
within your analytics system, this plan should:
• Discuss the chosen input data set, its storage format and logical structure
o Where is it from
▪ e.g., Is it from a local database? A file? A web service?
o How is it stored?
▪ e.g., What kind of database? How is data physically stored using that database
engine?
▪ e.g., What kind of file? How is data physically stored within that file on the file
system?
o What is its logical structure?
▪ e.g., Is this source data a “flat file”? a collection of tables from a relational
database?
▪ What does it “look” like structurally? Document this
• Show the logical structure of the input data using an appropriate diagram
such as an ERD
• Detail and justify an appropriate logical and physical structure for the storage of data when it has
been ingested
o Physical structure
o Logical structure
▪ How will it be represented and structured logically to the Data Engineers and Data
Scientists who work with it?
▪ What benefits and drawbacks will this structure provide?
▪ What alternative structures could be used for the data?
• One Big Table?
• Dimensional Model?
- Star Schema
- Snowflake Schema
- etc.
• Normalised Relational Model?
▪ What are the benefits and drawbacks of these alternative structures?
• Detail the process of how data will be extracted, loaded (and possibly transformed) from the
original data source before being stored for later use by the analytics system
You should provide a detailed, well-justified discussion of each of these components
Tip
The design and development process of real-world systems are often iterative, this means that
you are likely to benefit from refining your process throughout the development lifecycle.
Therefore, this task should not be seen as a static “tick-box” document, instead, it should be
reviewed throughout your progress with the module assessment and iteratively refined as you
gain a better understanding of how to appropriately tackle the problem. Importantly, later more
refined versions of your plan can then be compared against earlier versions
CMP6230 Coursework Task 1 (2024/2025)
S Iranmanesh 10/2024 – Page 7 of 7
Marking
Marks will be awarded within this task based on:
• Successful completion of a precisely specified, and well-justified plan for a data analytics /
machine learning pipeline accompanied by a sufficiently detailed discussion
◦ How well have you presented and justified your pipeline plan, and discussed the detailed
steps that would be involved when implementing it?
▪ Try to show your understanding of what needs to happen at each stage of the process
▪ Want good marks? Then you should focus on showing us your growing understanding of
the ‘whats’, ‘whys’, ‘whos’ and ‘hows’ involved in the planning of this pipeline
◦ Have you presented high-quality, easy-to-follow, evidence of your plans in the form of
diagrams, figures, tables, etc?
◦ How well have you discussed the process of iteratively refining your pipeline as you progress
throughout the assessment?
• Successful completion of a precisely specified, and well-justified plan for the appropriate
storage of data for use within this pipeline, accompanied by a sufficiently detailed discussion
◦ How well have you presented and justified your proposed storage plan, and discussed the
detailed steps that would be involved when implementing it?
▪ Try to show your understanding of what needs to happen at each stage of the process
▪ Want good marks? Then you should focus on showing us your growing understanding of
the ‘whats’, ‘whys’, ‘whos’ and ‘hows’ involved in the planning of this pipeline
◦ Have you presented high-quality, easy-to-follow, evidence of your proposed storage plan in
the form of diagrams, figures, tables, etc?
◦ How well have you discussed the benefits and drawbacks of your storage plan?
▪ The logical and physical structures imposed upon the data will always introduce some
trade-offs, what are they? why are they acceptable for your intended use of the data?
• A reflective discussion of the planning process undertaken including:
◦ What have you learned from undertaking the process? (technical skills, knowledge and more
general skills)
◦ How have you approached iteratively refining your plan
◦ How could you improve if undertaking this process in the future?

## Building the CMP6230 PDF (optional)

Requires a TeX distribution with **latexmk**, **pdfLaTeX**, **biblatex**, and **biber**.

Diagram exports from Draw.io (and similar) are often PDF~1.7 with complex object streams; **pdfLaTeX** can hit a fixed stream buffer when embedding them. The checked-in diagram files under `figures/` are re-encoded to PDF~1.5 with Ghostscript so `make pdf` works reliably. If you replace a diagram and the build fails with `TeX capacity exceeded` / `PDF object stream buffer`, run:

```bash
cd report
bash scripts/normalize-figure-pdfs.sh
make pdf
```

Alternatively, compile with **LuaLaTeX** (often tolerates the originals): `latexmk -lualatex main.tex`.

```bash
cd report
make pdf
```

The PDF is produced as `report/build/main.pdf`. Use `make watch` for continuous rebuilds (`latexmk -pvc`).