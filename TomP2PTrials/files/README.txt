Creating the JARs
----------------------
Run "mvn package -Dmaven.test.skip=true" in project's root directory.


Running the program
----------------------
Go the the project's build directory (target).

Example Bootstrapper:
	java -jar Bootstrapper.jar configBootstrapper.json
Example Client:
	java -jar Client.jar configAlice.json



