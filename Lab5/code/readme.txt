Details about the bigrams and trigrams:

The wordpair.py and wordtriplet.py provided in the folder code can be used for generating bigrams and trigrams respectively, using spark. These python files expect a sample folder in the same directory. This sample folder should contain the files that are to be run to generate bigrams and trigrams. These python files need new_lemmatizer.csv file in the same directory to generate lemmas.Both these files generate output folders with names sample_op_wp and sample_op_wt respectively. These folders would inturn contain another folder with timestamp as the name and the results are stores inside this folder. 

Output format:

The output of wordpair will be of the format
((pair), location)
Example:
(('acus', 'tunico'), u'<verg. aen. 11.777>')

The output of wordtriplet will be of the format
((triplet), location)
Example:
(('ego', 'conficio', 'cures'), u'<verg. aen. 6.520>')