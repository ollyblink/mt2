function process(keyIn, valuesIn, context){ 
	for each (var value in valuesIn) {   
		var splits = value.split(" ");	   
		for each (var split in splits){       
			context.write(split, 1);	    
		}  
	}	
}