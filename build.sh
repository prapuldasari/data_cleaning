mkdir -p dependencies
for i in $(cat dependency_list1.txt); do
    cp "$i" dependencies/
done

zip dependencies.zip dependencies/*

rm -r dependencies
