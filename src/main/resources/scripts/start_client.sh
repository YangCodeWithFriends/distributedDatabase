# YCQL
ssh cs4224j@xcnd20.comp.nus.edu.sg \
    "sh -c 'cd ~/target && rm YCQL-log-*.txt'"

ssh cs4224j@xcnd20.comp.nus.edu.sg \
    "sh -c 'cd ~/target && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YCQL 0 &'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
    "sh -c 'cd ~/target && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YCQL 1 &'"

ssh cs4224j@xcnd22.comp.nus.edu.sg \
    "sh -c 'cd ~/target && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YCQL 2 &'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
    "sh -c 'cd ~/target && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YCQL 3 &'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
    "sh -c 'cd ~/target && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YCQL 4 &'"

# --- break line ---

# YSQL
ssh cs4224j@xcnd20.comp.nus.edu.sg \
    "sh -c 'cd ~/target && rm YSQL-log-*.txt'"

ssh cs4224j@xcnd20.comp.nus.edu.sg \
    "sh -c 'cd ~/target && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YSQL 0 &'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
    "sh -c 'cd ~/target && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YSQL 1 &'"

ssh cs4224j@xcnd22.comp.nus.edu.sg \
    "sh -c 'cd ~/target && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YSQL 2 &'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
    "sh -c 'cd ~/target && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YSQL 3 &'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
    "sh -c 'cd ~/target && java -cp yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp YSQL 4 &'"