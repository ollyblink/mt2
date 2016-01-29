function process(keyIn, valuesIn, context){  
	var count = 0;   
	for each (var value in valuesIn) {   
		count += value;	  
	}   
	context.write(keyIn, count); 
} 