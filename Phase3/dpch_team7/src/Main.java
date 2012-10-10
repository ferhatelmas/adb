import java.util.HashMap;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length<1){
            printHelp();
            return;
        }
        String[] argcopy = new String[args.length-1];
        System.arraycopy(args, 1, argcopy, 0, argcopy.length);
        if (args[0].equals("validator"))
            Validator.main(argcopy);
        else if (args[0].equals("browser"))
            olap_datacube.CubeBrowsingInterface.main(argcopy);
        else if (args[0].equals("build"))
            olap_datacube.CubeBuilder.main(argcopy);
        else if (args[0].equals("operations"))
            olap_datacube.CubeOperations.main(argcopy);
        else if (args[0].equals("parameter-set"))
            olap_datacube.CubeParameterSet.main(argcopy);
        else if (args[0].equals("sshjtest"))
            SSHJTest.main(argcopy);
        else {
            printHelp();
        }
    }

    static void printHelp(){
        System.out.println("Please specify a command (and append arguments if appropriate):");
        System.out.println("validator\t\t(?)");
        System.out.println("browser\t\t\tCube browser (must have built first)");
        System.out.println("build\t\t\tBuild data cube");
        System.out.println("operations\t\ttest-case");
        System.out.println("parameter-set\t\ttest-case");
        System.out.println("sshjtest\t\ttest-case");
    }

}
