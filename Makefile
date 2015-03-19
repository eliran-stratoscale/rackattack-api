all: check_convention

check_convention:
	pep8 py test* --max-line-length=109

#this can not run in dirbalak clean build, as solvent can not yet be run at this point (still running in 
#rootfs-build-nostrato, this project is a dependency of rootfs-buid). This is why this is actually being
#run in pyracktest
delayed_racktest:
	UPSETO_JOIN_PYTHON_NAMESPACES=yes PYTHONPATH=$(PWD):$(PWD)/py python test/test.py
virttest:
	RACKATTACK_PROVIDER=tcp://localhost:1014@@amqp://guest:guest@localhost:1013/%2F@@http://localhost:1016 $(MAKE) delayed_racktest
phystest:
	RACKATTACK_PROVIDER=tcp://rackattack-provider:1014@@amqp://guest:guest@rackattack-provider:1013/%2F@@http://rackattack-provider:1016 $(MAKE) delayed_racktest
