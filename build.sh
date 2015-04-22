ps aux | awk '/[h]ive/ { print $2 }' | xargs kill
mvn clean install -Phadoop-2,dist -DskipTests
