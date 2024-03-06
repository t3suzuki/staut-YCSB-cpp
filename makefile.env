
init:
	echo 0 | sudo tee -a /proc/sys/vm/mmap_min_addr
	sleep 1
	echo 2048 | sudo tee -a /proc/sys/vm/nr_hugepages
	sleep 1
	-sudo umount mountpoint
	sleep 1
	-sudo lvchange -a n vg1
	sleep 1
	-sudo vgchange -a n vg1
	sleep 1
	-sudo pvchange -x n /dev/nvme3n2	
	sleep 1
	-sudo pvchange -x n /dev/nvme4n2	
	sleep 1
	make setup PCIADDR=0000:0f:00.0
	sleep 1
	make setup PCIADDR=0000:0e:00.0

revert:
	make reset PCIADDR=0000:0f:00.0
	sleep 1
	make reset PCIADDR=0000:0e:00.0
	sleep 1
	sudo pvchange -x y /dev/nvme3n2
	sleep 1
	sudo pvchange -x y /dev/nvme4n2	
	sleep 1
	sudo vgchange -a y vg1
	sleep 1
	sudo lvchange -a y vg1
	sleep 1
	sudo mount /dev/vg1/striped /home/tomoya-s/mountpoint/
	echo 0 | sudo tee -a /sys/block/nvme3n2/queue/iostats
	echo 0 | sudo tee -a /sys/block/nvme4n2/queue/iostats
	echo 2 | sudo tee -a /sys/block/nvme3n2/queue/nomerges
	echo 2 | sudo tee -a /sys/block/nvme4n2/queue/nomerges
	echo 0 | sudo tee -a /sys/block/nvme3n2/queue/wbt_lat_usec
	echo 0 | sudo tee -a /sys/block/nvme4n2/queue/wbt_lat_usec

setup:
	sudo modprobe uio_pci_generic
	echo $(PCIADDR) | sudo tee -a /sys/bus/pci/devices/$(PCIADDR)/driver/unbind
	echo uio_pci_generic | sudo tee -a /sys/bus/pci/devices/$(PCIADDR)/driver_override
	echo $(PCIADDR) | sudo tee -a /sys/bus/pci/drivers_probe
reset:
	echo $(PCIADDR) | sudo tee -a /sys/bus/pci/devices/$(PCIADDR)/driver/unbind
	echo nvme | sudo tee -a /sys/bus/pci/devices/$(PCIADDR)/driver_override
	echo $(PCIADDR) | sudo tee -a /sys/bus/pci/drivers_probe
