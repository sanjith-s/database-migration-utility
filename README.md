<br/>
<p align="center">
  <h3 align="center">Database Migrator</h3>

  <p align="center">
    Java based Command Line tool for relational database migration
    <br/>
    <br/>
  </p>
</p>



## Table Of Contents

* [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Usage](#usage)
* [Authors](#authors)
* [Acknowledgements](#acknowledgements)

## Built With

This tool is implemented Spring framework and uses Maven Build system

## Getting Started

You can run this tool directly from your IDE or use to jar file and the shell script to run it from your terminal

### Prerequisites

JDK >= 8

### Installation

1. Clone the repo

```sh
git clone https://github.com/sanjith-s/database-migration-utility.git
```

2. Build the project using maven

```sh
mvn clean install
```

3. Run the jar file using run.sh script

```sh
run.sh
```

## Usage

This tool assumes that the destination table has been created by the user and other related tables connected by foreign key constraints. Users can turn off integrity checks at the destination table while using this tool.



## Authors

* **Sanjith S** 
