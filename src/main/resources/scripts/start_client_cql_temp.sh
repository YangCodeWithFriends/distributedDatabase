# YCQL
ssh cs4224j@xcnd20.comp.nus.edu.sg \
    "sh -c 'cd ~/target_temp && rm YCQL-log-*'" &

ssh cs4224j@xcnd20.comp.nus.edu.sg \
    "sh -c 'cd ~/target_temp && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YCQL 0 &'" &

ssh cs4224j@xcnd21.comp.nus.edu.sg \
    "sh -c 'cd ~/target_temp && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YCQL 1 &'" &

ssh cs4224j@xcnd22.comp.nus.edu.sg \
    "sh -c 'cd ~/target_temp && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YCQL 2 &'" &

ssh cs4224j@xcnd23.comp.nus.edu.sg \
    "sh -c 'cd ~/target_temp && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YCQL 3 &'" &

ssh cs4224j@xcnd24.comp.nus.edu.sg \
    "sh -c 'cd ~/target_temp && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YCQL 4 &'" &