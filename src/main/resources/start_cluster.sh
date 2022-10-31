# run master
ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'cd ~/yugabyte-2.14.1.0/bin && ./yb-master --flagfile 2ms20.conf > /mnt/ramdisk/gj2/yb-master.out 2>&1 &'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'cd ~/yugabyte-2.14.1.0/bin && ./yb-master --flagfile 2ms21.conf > /mnt/ramdisk/gj2/yb-master.out 2>&1 &'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'cd ~/yugabyte-2.14.1.0/bin && ./yb-master --flagfile 2ms23.conf > /mnt/ramdisk/gj2/yb-master.out 2>&1 &'"

# run t-server
ssh cs4224j@xcnd20.comp.nus.edu.sg \
"sh -c 'cd ~/yugabyte-2.14.1.0/bin && ./yb-tserver --flagfile 2ts20.conf > /mnt/ramdisk/gj2/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd21.comp.nus.edu.sg \
"sh -c 'cd ~/yugabyte-2.14.1.0/bin && ./yb-tserver --flagfile 2ts21.conf > /mnt/ramdisk/gj2/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd22.comp.nus.edu.sg \
"sh -c 'cd ~/yugabyte-2.14.1.0/bin && ./yb-tserver --flagfile 2ts22.conf > /mnt/ramdisk/gj2/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd23.comp.nus.edu.sg \
"sh -c 'cd ~/yugabyte-2.14.1.0/bin && ./yb-tserver --flagfile 2ts23.conf > /mnt/ramdisk/gj2/yb-tserver.out 2>&1 &'"

ssh cs4224j@xcnd24.comp.nus.edu.sg \
"sh -c 'cd ~/yugabyte-2.14.1.0/bin && ./yb-tserver --flagfile 2ts24.conf > /mnt/ramdisk/gj2/yb-tserver.out 2>&1 &'"
