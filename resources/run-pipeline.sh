#! /bin/bash
echo "starting the service ..."
python pipeline.py --input_subs $PROJ_SUBS --project $PROJ_NAME --kind $PROJ_DBKIND --streaming

# wait forever not to exit the container
#while true
#do
#  tail -f /dev/null & wait ${!}
#done

exit 0