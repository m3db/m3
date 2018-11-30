# Specifications

This directory contains specifications for various M3 components that are used for documentation and/or verification.

## TLA+ Specifications

TLA+ specifications in this folder are written in [PlusCal](https://lamport.azurewebsites.net/tla/pluscal.html) and then automatically translated into TLA+ and checked with the TLC model checker using the [TLA+ toolbox](https://lamport.azurewebsites.net/tla/toolbox.html).

Development instructions:

1. Install and open the TLA+ toolbox [TLA+ toolbox](https://lamport.azurewebsites.net/tla/toolbox.html).
2. Click `File` and then `Open Spec` and finally `Add New Spec`.
3. Name the file / module exactly the same as it is named in the `MODULE` declaration of the top of the specification you wish to run.
4. Paste the specification into the code editor.
5. Click `TLC Model Checker` then `New Model` and name the model whatever you want.
6. In the `Model Overview` section, specify the values of any pre-declared constants. Review the comments in the specification for guidance on choosing values. When in doubt, pick small values that limit the search space of the model checker and gradually increase them after measuring how long it takes the model checker to evaluate the specification.
7. Add any named invariants to the `Invariants` section under the `What to check?` panel in the `Model Overview` tab. By convention, these invariants are listed at the bottom of the specification, underneat the auto-generated TLA+ translation.
8. Click the green play symbol to check the model.

Note that if you change the PlusCal algorithm you will need to regenerate the TLA+ translation by clicking `File` and then `Translate PlusCal Algorithm`.