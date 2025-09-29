# Vaporous
Vaporous provides a chained query syntax for accessing unstructured data and converting it into interpretable analytics. 

The tool is still in its early phases of development and is missing quality of life features for query writers



## Examples
You can find example queries [via git in the examples folder](https://github.com/lkashl/vaporous/tree/main/examples). 

## TODO List
- Support web page embedded Vaporous so clients can use browser folder storage as file input
- Add an error for if a user tries to generate a graph without first calling toGraph
- Intercept structual errors earlier and add validation to functions - not necessarily data as this casues overhaead
- Migrate reponsibility for tabular conversion from create element to the primary library to reduce overhead of graph generation

