all: check_convention unittest

unittest:
	UPSETO_JOIN_PYTHON_NAMESPACES=Yes PYTHONPATH=py python -m coverage run py/rackattack/tests/runner.py
	python -m coverage report --show-missing --include=py/*.py --omit=py/rackattack/tests/*.py,*/__init__.py

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
	RACKATTACK_PROVIDER=tcp://rackattack-provider.dc1.strato:1014@@amqp://guest:guest@rackattack-provider.dc1.strato:1013/%2F@@http://rackattack-provider.dc1.strato:1016 $(MAKE) delayed_racktest
