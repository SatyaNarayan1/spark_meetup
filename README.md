# Spark Meetup
Example code from IBM spark meetup "Deep dive into RDD"

### Prerquisite 
* Scala
* Sbt
* Intellij/Eclipse

### Getting code

           git clone https://github.com/SatyaNarayan1/spark_meetup.git

### Build & Run

        sbt clean package
        sbt "run-main <Main Class Name> <args>"
        ex: sbt "run-main Lineage local[4] /path/to/README.md"

