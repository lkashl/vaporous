# Vaporous
Vaporous provides a chained query syntax for accessing unstructured data and converting it into interpretable analytics

The tool is still in its early phases of development and is missing some quality of life features

The query syntax is heavily inspired by splunk with more bias towards programmitic functionality

## Examples

Interactive previews for two datasources are available

- [Virtualised temperature sensor data](https://lkashl.github.io/vaporous/pages/temp_sensors.html)
- [CSV delimited virtruvian data](https://lkashl.github.io/vaporous/pages/gym.html)

Examples of the source queries used can be referenced in the [examples folder](https://github.com/lkashl/vaporous/tree/main/examples)

## TODO List
- Support web page embedded Vaporous so clients can use browser folder storage as file input
- Add an error for if a user tries to generate a graph without first calling toGraph
- Intercept structual errors earlier and add validation to functions - not necessarily data as this casues overhaead
- Migrate reponsibility for tabular conversion from create element to the primary library to reduce overhead of graph generation

